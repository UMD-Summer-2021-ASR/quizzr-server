import json
import logging
import os
import random
import sys
from datetime import datetime
from http import HTTPStatus

import jsonschema.exceptions
import pymongo.errors
from firebase_admin import auth
from fuzzywuzzy import fuzz

import bson.json_util
from flask import Flask, request, render_template, send_file, make_response
from flask_cors import CORS
from openapi_schema_validator import validate
from werkzeug.exceptions import abort

import error_handling
import rec_processing
from sv_api import QuizzrAPISpec
from tpm import QuizzrTPM
from sv_errors import UserExistsError, ProfileNotFoundError, MalformedProfileError

logging.basicConfig(level=os.environ.get("QUIZZR_LOG") or "DEBUG")
logger = logging.getLogger(__name__)

DEV_ENV_NAME = "development"
PROD_ENV_NAME = "production"
TEST_ENV_NAME = "testing"


def create_app(test_overrides=None, test_inst_path=None):
    """App factory function for the data flow server"""
    instance_path = test_inst_path or os.environ.get("Q_INST_PATH")\
        or os.path.expanduser(os.path.join("~", "quizzr_server"))
    app = Flask(
        __name__,
        instance_relative_config=True,
        instance_path=instance_path
    )
    CORS(app)
    server_dir = os.path.dirname(__file__)
    os.makedirs(app.instance_path, exist_ok=True)
    path = app.instance_path
    default_config = {
        "UNPROC_FIND_LIMIT": 32,
        "DATABASE": "QuizzrDatabase",
        "BLOB_ROOT": "production",
        "BLOB_NAME_LENGTH": 32,
        "Q_ENV": PROD_ENV_NAME,
        "SUBMISSION_FILE_TYPES": ["wav", "json", "vtt"],
        "DIFFICULTY_LIMITS": [3, 6, None],
        "VERSION": "0.2.0",
        "MIN_ANSWER_SIMILARITY": 50,
        "PROC_CONFIG": {
            "checkUnk": True,
            "unkToken": "<unk>",
            "minAccuracy": 0.5,
            "queueLimit": 32
        },
        "DEV_UID": "dev",
        "LOG_PRIVATE_DATA": False,
        "VISIBILITY_CONFIGS": {
            "basic": {
                "projection": {"pfp": 1, "username": 1, "usernameSpecs": 1},
                "collection": "Users"
            },
            "public": {
                "projection": {"pfp": 1, "username": 1, "usernameSpecs": 1},
                "collection": "Users"
            },
            "private": {
                "projection": {"_id": 0},
                "collection": "Users"
            }
        },
        "USE_ID_TOKENS": True
    }

    config_dir = os.path.join(path, "config")
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    conf_path = os.path.join(config_dir, "sv_config.json")

    app_conf = default_config
    if os.path.exists(conf_path):
        with open(conf_path, "r") as config_f:
            config = json.load(config_f)
        app_conf.update(config)

    for var in app_conf:
        if type(app_conf[var]) is not str and var in os.environ:
            app.logger.critical(f"Cannot override non-string config field '{var}' through environment variable")
            sys.exit(1)
    app_conf.update(os.environ)
    if test_overrides:
        app_conf.update(test_overrides)

    rec_dir = os.path.join(app.instance_path, "storage", "queue")
    if not os.path.exists(rec_dir):
        os.makedirs(rec_dir)

    secret_dir = os.path.join(app.instance_path, "secrets")
    if not os.path.exists(secret_dir):
        os.makedirs(secret_dir)

    app_conf["REC_DIR"] = rec_dir
    api = QuizzrAPISpec(os.path.join(server_dir, "reference", "backend.yaml"))

    app.config.from_mapping(app_conf)
    qtpm = QuizzrTPM(app_conf["DATABASE"], app_conf, secret_dir, rec_dir, api)
    qp = rec_processing.QuizzrProcessor(
        qtpm.database,
        rec_dir,
        app_conf["PROC_CONFIG"],
        app_conf["SUBMISSION_FILE_TYPES"]
    )
    # TODO: multiprocessing

    @app.route("/audio", methods=["GET", "POST", "PATCH"])
    def audio_resource():
        """
        GET: Get a batch of at most UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB
        Atlas.

        POST: Submit a recording for pre-screening and upload it to the database if it passes.

        PATCH: Attach the given arguments to multiple unprocessed audio documents and move them to the Audio collection.
        Additionally, update the recording history of the associated questions and users.
        """
        if request.method == "GET":
            return get_unprocessed_audio()
        elif request.method == "POST":
            decoded = _verify_id_token()
            recording = request.files.get("audio")
            question_id = request.form.get("qid")
            diarization_metadata = request.form.get("diarMetadata")
            rec_type = request.form.get("recType")
            user_id = decoded['uid']
            return pre_screen(recording, question_id, user_id, diarization_metadata, rec_type)
        elif request.method == "PATCH":
            arguments_batch = request.get_json()
            return handle_processing_results(arguments_batch)

    def get_unprocessed_audio():
        """Get a batch of at most UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB
        Atlas."""
        max_docs = app.config["UNPROC_FIND_LIMIT"]
        errs = []
        results_projection = ["_id", "diarMetadata"]  # In addition to the transcript

        app.logger.info(f"Finding a batch ({max_docs} max) of unprocessed audio documents...")
        audio_doc_count = qtpm.unproc_audio.count_documents({"_id": {"$exists": True}}, limit=max_docs)
        if audio_doc_count == 0:
            app.logger.error("Could not find any audio documents")
            return "empty_unproc_audio", HTTPStatus.NOT_FOUND
        audio_cursor = qtpm.unproc_audio.find(limit=max_docs)
        qid2entries = {}
        for audio_doc in audio_cursor:
            app.logger.debug(f"audio_doc = {audio_doc!r}")

            qid = audio_doc.get("questionId")
            if qid is None:
                app.logger.warning("Audio document does not contain question ID")
                errs.append(("internal_error", "undefined_question_id"))
                continue
            if qid not in qid2entries:
                qid2entries[qid] = []
            entry = {}
            for field in results_projection:
                if field in audio_doc:
                    entry[field] = audio_doc[field]
            qid2entries[qid].append(entry)

        qids = list(qid2entries.keys())
        app.logger.debug(f"qid2entries = {qid2entries!r}")
        app.logger.info(f"Found {audio_doc_count} unprocessed audio document(s)")
        if not qids:
            app.logger.error("No audio documents contain question IDs")
            return "empty_qid2entries", HTTPStatus.NOT_FOUND

        questions = qtpm.find_questions(qids)
        for question in questions:
            qid = question["_id"]
            transcript = question.get("transcript")
            if transcript:
                entries = qid2entries[qid]
                for entry in entries:
                    entry["transcript"] = transcript

        app.logger.debug(f"qid2entries = {qid2entries!r}")

        results = []
        for entries in qid2entries.values():
            results += entries
        app.logger.debug(f"Final Results: {results!r}")
        response = {"results": results}
        if errs:
            response["errors"] = [{"type": err[0], "reason": err[1]} for err in errs]
        return response

    def pre_screen(recording, question_id, user_id, diarization_metadata, rec_type):
        """Submit a recording for pre-screening and upload it to the database if it passes."""
        valid_rec_types = ["normal", "buzz"]

        app.logger.debug(f"question_id = {question_id!r}")
        app.logger.debug(f"diarization_metadata = {diarization_metadata!r}")
        app.logger.debug(f"rec_type = {rec_type!r}")

        if not recording:
            app.logger.error("No audio recording defined. Aborting")
            return "arg_audio_undefined", HTTPStatus.BAD_REQUEST

        if not rec_type:
            app.logger.error("Form argument 'recType' is undefined. Aborting")
            return "arg_recType_undefined", HTTPStatus.BAD_REQUEST
        elif rec_type not in valid_rec_types:
            app.logger.error(f"Invalid rec type {rec_type!r}. Aborting")
            return "arg_recType_invalid", HTTPStatus.BAD_REQUEST
        qid_required = rec_type != "buzz"
        qid_used = qid_required or rec_type != "buzz"  # Redundant but for flexibility

        question_id, success = error_handling.to_oid_soft(question_id)
        if qid_used:
            if qid_required and not question_id:
                app.logger.error("Form argument 'qid' expected. Aborting")
                return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
            if not success:
                app.logger.error("Form argument 'qid' is not a valid ObjectId. Aborting")
                return "arg_qid_invalid", HTTPStatus.BAD_REQUEST

        # user_ids = QuizzrTPM.get_ids(qtpm.users)
        # if not user_ids:
        #     app.logger.error("No user IDs found. Aborting")
        #     return "empty_uids", HTTPStatus.INTERNAL_SERVER_ERROR
        # user_ids = qtpm.user_ids.copy()
        # while True:
        #     user_id, success = error_handling.to_oid_soft(random.choice(user_ids))
        #     if success:
        #         break
        #     app.logger.warning(f"Found malformed user ID {user_id}. Retrying...")
        #     user_ids.remove(user_id)
        #     if not user_ids:
        #         app.logger.warning("Could not find properly formed user IDs. Proceeding with last choice")
        #         break
        app.logger.debug(f"user_id = {user_id!r}")

        metadata = {
            "recType": rec_type,
            "userId": user_id
        }
        if diarization_metadata:
            metadata["diarMetadata"] = diarization_metadata
        if question_id:
            metadata["questionId"] = question_id

        submission_name = _save_recording(rec_dir, recording, metadata)
        app.logger.debug(f"submission_name = {submission_name!r}")
        try:
            # accepted_submissions, errors = qp.pick_submissions(
            #     rec_processing.QuizzrWatcher.queue_submissions(qtpm.REC_DIR)
            # )
            results = qp.pick_submissions(
                rec_processing.QuizzrWatcher.queue_submissions(
                    app.config["REC_DIR"],
                    size_limit=app.config["PROC_CONFIG"]["queueLimit"]
                )
            )
        except BrokenPipeError as e:
            app.logger.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR

        # Upload files
        normal_file_paths = []
        buzz_file_paths = []
        buzz_submissions = []
        for submission in results:
            file_path = os.path.join(app.config["REC_DIR"], submission) + ".wav"
            if results[submission]["case"] == "accepted":
                if results[submission]["metadata"]["recType"] == "buzz":
                    buzz_file_paths.append(file_path)
                    buzz_submissions.append(submission)
                else:
                    normal_file_paths.append(file_path)
        try:
            file2blob = qtpm.upload_many(normal_file_paths, "normal")
            file2blob.update(qtpm.upload_many(buzz_file_paths, "buzz"))
        except BrokenPipeError as e:
            app.logger.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR

        # sub2blob = {os.path.splitext(file)[0]: file2blob[file] for file in file2blob}
        sub2meta = {}
        sub2vtt = {}

        for submission in results:
            doc = results[submission]
            if doc["case"] == "accepted":
                sub2meta[submission] = doc["metadata"]
                if "vtt" in doc:
                    sub2vtt[submission] = doc.get("vtt")

        # Insert submissions
        qtpm.mongodb_insert_submissions(
            sub2blob={os.path.splitext(file)[0]: file2blob[file] for file in file2blob},
            sub2meta=sub2meta,
            sub2vtt=sub2vtt,
            buzz_submissions=buzz_submissions
        )

        for submission in results:
            rec_processing.delete_submission(app.config["REC_DIR"], submission, app.config["SUBMISSION_FILE_TYPES"])

        if results[submission_name]["case"] == "accepted":
            return {"prescreenSuccessful": True}, HTTPStatus.ACCEPTED

        if results[submission_name]["case"] == "err":
            return results[submission_name]["err"], HTTPStatus.INTERNAL_SERVER_ERROR

        return {"prescreenSuccessful": False}, HTTPStatus.ACCEPTED

    def handle_processing_results(arguments_batch):
        """Attach the given arguments to multiple unprocessed audio documents and move them to the Audio collection.
        Additionally, update the recording history of the associated questions and users."""
        arguments_list = arguments_batch.get("arguments")
        if arguments_list is None:
            return "undefined_arguments", HTTPStatus.BAD_REQUEST
        app.logger.info(f"Updating data related to {len(arguments_list)} audio documents...")
        errors = []
        success_count = 0
        for arguments in arguments_list:
            errs = qtpm.update_processed_audio(arguments)
            if not errs:
                success_count += 1
            else:
                errors += ({"type": err[0], "reason": err[1]} for err in errs)

        results = {"successes": success_count, "total": len(arguments_list)}
        if errors:
            results["errors"] = errors
        app.logger.info(f"Successfully updated data related to {success_count} of {len(arguments_list)} audio documents")
        app.logger.info(f"Logged {len(errors)} warning messages")
        return results

    @app.route("/answer", methods=["GET"])
    def check_answer():
        """Check if an answer is correct."""
        qid = request.args.get("qid")
        user_answer = request.args.get("a")

        if not qid:
            return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
        if not user_answer:
            return "arg_a_undefined", HTTPStatus.BAD_REQUEST

        question = qtpm.rec_questions.find_one({"_id": bson.ObjectId(qid)})
        correct_answer = question.get("answer")
        if not correct_answer:
            return "answer_not_found", HTTPStatus.NOT_FOUND

        return {"correct": fuzz.token_set_ratio(user_answer, correct_answer) >= app.config["MIN_ANSWER_SIMILARITY"]}

    @app.route("/audio/<path:blob_path>", methods=["GET"])
    def retrieve_audio_file(blob_path):
        """Retrieve a file from Firebase Storage."""
        return send_file(qtpm.get_file_blob(blob_path), mimetype="audio/wav")

    @app.route("/profile", methods=["GET", "POST", "PATCH", "DELETE"])
    def own_profile():
        """A resource to automatically point to the user's own profile."""
        decoded = _verify_id_token()
        # decoded = {"uid": "dev"}
        user_id = decoded["uid"]
        if request.method == "GET":
            return qtpm.get_profile(user_id, "private")
        elif request.method == "POST":
            args = request.get_json()
            path, op = api.path_for("modify_profile")
            schema = api.build_schema(api.api["paths"][path][op]["requestBody"]["content"]["application/json"]["schema"])
            err = _validate_args(args, schema)
            if err:
                return err
            try:
                result = qtpm.create_profile(user_id, args["pfp"], args["username"])
            except pymongo.errors.DuplicateKeyError:
                app.logger.error("User already registered. Aborting")
                return "already_registered", HTTPStatus.BAD_REQUEST
            except UserExistsError as e:
                app.logger.error(f"User already exists {e}. Aborting")
                return f"user_exists: {e}", HTTPStatus.BAD_REQUEST
            if result:
                app.logger.info("User successfully created")
                return "user_created", HTTPStatus.CREATED
            app.logger.error("Encountered an unknown error")
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR
        elif request.method == "PATCH":
            path, op = api.path_for("modify_profile")
            update_args = request.get_json()
            schema = api.build_schema(api.api["paths"][path][op]["requestBody"]["content"]["application/json"]["schema"])
            err = _validate_args(update_args, schema)
            if err:
                return err
            result = qtpm.modify_profile(user_id, update_args)
            if result:
                return "user_modified", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR
        elif request.method == "DELETE":
            qtpm.delete_profile(user_id)
            result = qtpm.delete_profile(user_id)
            if result:
                return "user_deleted", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR

    @app.route("/profile/<username>", methods=["GET", "PATCH", "DELETE"])
    def other_profile(username):
        other_user_profile = qtpm.users.find_one({"username": username}, {"_id": 1})
        if not other_user_profile:
            abort(HTTPStatus.NOT_FOUND)
        other_user_id = other_user_profile["_id"]
        if request.method == "GET":
            visibility = "basic" if _query_flag("basic") else "public"
            return qtpm.get_profile(other_user_id, visibility)
        elif request.method == "PATCH":
            update_args = request.get_json()
            result = qtpm.modify_profile(other_user_id, update_args)
            if result:
                return "other_user_modified", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR
        elif request.method == "DELETE":
            result = qtpm.delete_profile(other_user_id)
            if result:
                return "other_user_deleted", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR

    @app.route("/question", methods=["GET"])
    def pick_game_question():
        """Find a random recorded question and return the VTT and ID of the recording with the best evaluation."""
        question_ids = QuizzrTPM.get_ids(qtpm.rec_questions)
        if not question_ids:
            app.logger.error("No recorded questions found. Aborting")
            return "rec_empty_qids", HTTPStatus.NOT_FOUND

        # question_ids = qtpm.rec_question_ids.copy()
        while True:
            next_question_id = random.choice(question_ids)
            next_question = qtpm.rec_questions.find_one({"_id": next_question_id})
            app.logger.debug(f"{type(next_question_id)} next_question_id = {next_question_id!r}")
            app.logger.debug(f"next_question = {next_question!r}")

            if next_question and next_question.get("recordings"):
                audio = qtpm.find_best_audio_doc(
                    next_question.get("recordings"),
                    required_fields=["_id", "vtt", "gentleVtt"]
                )
                if audio:
                    break
            app.logger.warning(
                f"ID {next_question_id} is invalid or associated question has no valid audio recordings")
            question_ids.remove(next_question_id)
            if not question_ids:
                app.logger.error("Failed to find a viable recorded question. Aborting")
                return "rec_corrupt_questions", HTTPStatus.NOT_FOUND
        result = audio.copy()
        result["qid"] = str(next_question_id)
        return result

    @app.route("/question/unrec", methods=["GET", "POST"])
    def unrec_question_resource():
        if request.method == "GET":
            difficulty = request.args.get("difficultyType")
            batch_size = request.args.get("batchSize")
            return pick_recording_question(difficulty, batch_size)
        elif request.method == "POST":
            arguments_batch = request.get_json()
            return upload_questions(arguments_batch)

    def pick_recording_question(difficulty, batch_size):
        """Find a random unrecorded question (or multiple) and return the ID and transcript."""
        if difficulty is not None:
            difficulty_query_op = QuizzrTPM.get_difficulty_query_op(app.config["DIFFICULTY_LIMITS"],
                                                                    int(difficulty))
            difficulty_query = {"recDifficulty": difficulty_query_op}
        else:
            difficulty_query = {}

        question_ids = QuizzrTPM.get_ids(qtpm.unrec_questions, difficulty_query)
        if not question_ids:
            app.logger.error(f"No unrecorded questions found for difficulty type '{difficulty}'. Aborting")
            return "unrec_empty_qids", HTTPStatus.NOT_FOUND

        errors = []
        if batch_size is not None and int(batch_size) > 1:
            next_questions, errors = qtpm.pick_random_questions(question_ids, int(batch_size))
            if next_questions is None:
                return "unrec_corrupt_questions", HTTPStatus.NOT_FOUND
            results = []
            for doc in next_questions:
                results.append({"transcript": doc["transcript"], "id": str(doc["_id"])})
        else:
            next_question = qtpm.pick_random_question(question_ids)
            if next_question is None:
                return "unrec_corrupt_questions", HTTPStatus.NOT_FOUND
            results = [{"transcript": next_question["transcript"], "id": str(next_question["_id"])}]
        return {"results": results, "errors": errors}

    def upload_questions(arguments_batch):
        """Upload a batch of unrecorded questions."""
        arguments_list = arguments_batch["arguments"]
        app.logger.debug(f"arguments_list = {arguments_list!r}")

        app.logger.info(f"Uploading {len(arguments_list)} unrecorded questions...")
        qtpm.insert_unrec_questions(arguments_list)
        app.logger.info("Successfully uploaded questions")
        return {"msg": "unrec_question.upload_success"}

    @app.route("/validate", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"])
    def check_token():
        """Endpoint for ORY Oathkeeper to validate a Bearer token. Accepts every standard method type because
        Oathkeeper also forwards the method type."""
        decoded = _verify_id_token()
        user_id = decoded["uid"]
        try:
            subject = _get_user_role(user_id)
        except ProfileNotFoundError as e:
            subject = "anonymous"
            app.logger.info(f"Profile not found: {e}. Subject registered as 'anonymous'")
        except MalformedProfileError as e:
            msg = str(e)
            app.logger.error(f"{msg}. Aborting")
            return f"{msg}\n", HTTPStatus.UNAUTHORIZED

        return {"subject": subject, "extra": {}}, HTTPStatus.OK

    # DO NOT INCLUDE THE ROUTES BELOW IN DEPLOYMENT
    @app.route("/uploadtest/")
    def recording_listener_test():
        if app.config["Q_ENV"] not in [DEV_ENV_NAME, TEST_ENV_NAME]:
            abort(HTTPStatus.NOT_FOUND)
        return render_template("uploadtest.html")

    def _get_next_submission_name():
        """Return a filename safe date-timestamp of the current system time"""
        return str(datetime.now().strftime("%Y.%m.%d %H.%M.%S.%f"))

    def _save_recording(directory, recording, metadata):
        """Write a WAV file and its JSON metadata to disk."""
        app.logger.info("Saving recording...")
        submission_name = _get_next_submission_name()
        app.logger.debug(f"submission_name = {submission_name!r}")
        submission_path = os.path.join(directory, submission_name)
        recording.save(submission_path + ".wav")
        app.logger.info("Saved recording successfully")

        app.logger.info("Writing metadata...")
        app.logger.debug(f"metadata = {metadata!r}")
        with open(submission_path + ".json", "w") as transaction_f:
            transaction_f.write(bson.json_util.dumps(metadata))
        app.logger.info("Successfully wrote metadata")
        return submission_name

    def _verify_id_token():
        """Try to decode the token if provided. If in a production environment, forbid access when the decode fails.
        This function is only callable in the context of a view function."""
        # return {"uid": app.config["DEV_UID"]}
        id_token = request.headers.get("Authorization")

        id_token_log = _get_private_data_string(id_token)
        app.logger.debug(f"id_token = {id_token_log}")

        if app.config["TESTING"] and not app.config["USE_ID_TOKENS"] and id_token:
            return {"uid": id_token}

        if not id_token:
            if app.config["Q_ENV"] == PROD_ENV_NAME:
                abort(HTTPStatus.UNAUTHORIZED)
            else:
                return {"uid": app.config["DEV_UID"]}

        # Cut off prefix if it is present
        prefix = "Bearer "
        if id_token.startswith(prefix):
            id_token = id_token[len(prefix):]

        try:
            decoded = auth.verify_id_token(id_token)
        except (auth.InvalidIdTokenError, auth.ExpiredIdTokenError) as e:
            logger.debug(repr(e))
            decoded = None
            abort(HTTPStatus.UNAUTHORIZED)
        except auth.CertificateFetchError:
            decoded = None
            abort(HTTPStatus.INTERNAL_SERVER_ERROR)

        return decoded

    def _get_private_data_string(original):
        """Utility function for redacting private data if 'LOG_PRIVATE_DATA' is not True"""
        if app.config["LOG_PRIVATE_DATA"]:
            return original
        return "[REDACTED]"

    def _query_flag(flag: str) -> bool:
        """
        Detect if a query 'flag' is present (e.g., "example.com/foo?bar" vs "example.com/foo"). Only callable in the
        context of a Flask view function.
        """
        return request.args.get(flag) is not None

    def _get_user_role(user_id):
        """
        Get the permission level of the user associated with the given ID

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :return: The role of the user
        :raise ProfileNotFoundError: When the profile associated with the given ID does not exist
        :raise MalformedProfileError: When the profile associated with the given ID is missing the permission level
        """
        profile = qtpm.users.find_one({"_id": user_id}, {"permLevel": 1})
        if not profile:
            raise ProfileNotFoundError(f"'{user_id}'")
        if "permLevel" not in profile:
            raise MalformedProfileError(f"Field 'permLevel' not found in profile for user '{user_id}'")
        return profile["permLevel"]

    def _validate_args(args: dict, schema: dict):
        """
        Shortcut for logic flow of schema validation when handling requests.

        :param args: The value to validate
        :param schema: The schema to use
        :return: An error response if the schema is invalid
        """
        try:
            validate(args, schema)
            err = None
        except jsonschema.exceptions.ValidationError as e:
            app.logger.error(f"Request arguments do not match schema: {e}")
            response = make_response(str(e))
            response.headers["Content-Type"] = "text/plain"
            err = (response, HTTPStatus.BAD_REQUEST)
        return err

    return app
