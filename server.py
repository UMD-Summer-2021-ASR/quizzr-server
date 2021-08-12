import atexit
import json
import logging
import logging.handlers
import multiprocessing
import os
import pprint
import re
import signal
from copy import deepcopy
from sys import exit
from datetime import datetime
from http import HTTPStatus
from secrets import token_urlsafe
from typing import List, Union, Tuple, Dict, Any, Optional

import jsonschema.exceptions
import pymongo.errors
import werkzeug.datastructures
from firebase_admin import auth
from fuzzywuzzy import fuzz

import bson.json_util
from flask import Flask, request, render_template, send_file, make_response
from flask_cors import CORS
from openapi_schema_validator import validate
from pymongo import UpdateOne
from werkzeug.exceptions import abort

import rec_processing
import sv_util
from sv_api import QuizzrAPISpec
from tpm import QuizzrTPM
from sv_errors import UsernameTakenError, ProfileNotFoundError, MalformedProfileError

logging.basicConfig(level=os.environ.get("QUIZZR_LOG") or "DEBUG")

DEV_ENV_NAME = "development"
PROD_ENV_NAME = "production"
TEST_ENV_NAME = "testing"

created_process = False


def create_app(test_overrides: dict = None, test_inst_path: str = None):
    """
    App factory function for the data flow server

    :param test_overrides: A set of overrides to merge on top of the server's configuration
    :param test_inst_path: The instance path of the server. Has the highest overriding priority
    :return: The data flow server as a Flask app
    """
    global created_process

    instance_path = test_inst_path or os.environ.get("Q_INST_PATH")\
        or os.path.expanduser(os.path.join("~", "quizzr_server"))
    app = Flask(
        __name__,
        instance_relative_config=True,
        instance_path=instance_path
    )
    log_dir = os.path.join(app.instance_path, "storage", "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_path = os.path.join(log_dir, "sv_log.log")
    handler = logging.handlers.TimedRotatingFileHandler(log_path, when='midnight', backupCount=7)
    app.logger.addHandler(handler)
    app.logger.info(f"Instantiated server with instance path '{instance_path}'")
    CORS(app)
    app.logger.info("Creating instance directory...")
    server_dir = os.path.dirname(__file__)
    os.makedirs(app.instance_path, exist_ok=True)
    app.logger.info("Created instance directory")
    app.logger.info("Configuring server instance...")
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
        "USE_ID_TOKENS": True,
        "MAX_LEADERBOARD_SIZE": 200,  # The maximum number of entries allowable on the leaderboard.
        "DEFAULT_LEADERBOARD_SIZE": 10  # The default number of entries on the leaderboard.
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
            exit(1)
    app_conf.update(os.environ)
    if test_overrides:
        app_conf.update(test_overrides)

    rec_dir = os.path.join(app.instance_path, "storage", "recordings")
    if not os.path.exists(rec_dir):
        os.makedirs(rec_dir)

    queue_dir = os.path.join(rec_dir, "queue")

    secret_dir = os.path.join(app.instance_path, "secrets")
    if not os.path.exists(secret_dir):
        os.makedirs(secret_dir)

    app_conf["REC_DIR"] = rec_dir
    api = QuizzrAPISpec(os.path.join(server_dir, "reference", "backend.yaml"))

    app.config.from_mapping(app_conf)
    if app.config["DEFAULT_LEADERBOARD_SIZE"] > app.config["MAX_LEADERBOARD_SIZE"]:
        app.logger.critical("Configured default leaderboard size must not exceed maximum leaderboard size.")
    app.logger.info("Finished configuring server instance")

    app.logger.info("Initializing third-party services...")
    app.logger.info(f"MongoDB Database Name = '{app_conf['DATABASE']}'")
    app.logger.info(f"Firebase Blob Root = '{app_conf['BLOB_ROOT']}'")
    app.logger.info(f"Environment set to '{app_conf['Q_ENV']}'")
    qtpm = QuizzrTPM(app_conf["DATABASE"], app_conf, api, os.path.join(secret_dir, "firebase_storage_key.json"))
    app.logger.info("Initialized third-party services")

    app.logger.info("Initializing pre-screening program...")
    # app.logger.debug("Instantiating QuizzrProcessorHead...")
    # qph = rec_processing.QuizzrProcessorHead(
    #     qtpm,
    #     rec_dir,
    #     app_conf["PROC_CONFIG"],
    #     app_conf["SUBMISSION_FILE_TYPES"]
    # )
    # app.logger.debug("Finished instantiating QuizzrProcessorHead")
    # app.logger.debug("Instantiating QuizzrWatcher...")
    # qw = rec_processing.QuizzrWatcher(os.path.join(rec_dir, "queue"), qph.execute)
    # app.logger.debug("Finished instantiating QuizzrWatcher")
    app.logger.debug("Instantiating process...")
    # qw_process = multiprocessing.Process(target=qw.execute)
    queue = multiprocessing.Queue()
    qw_process = multiprocessing.Process(target=rec_processing.start_watcher, kwargs={
        "db_name": app_conf["DATABASE"],
        "tpm_config": app_conf,
        "firebase_app_specifier": qtpm.app,
        "api": api,
        "rec_dir": rec_dir,
        "queue_dir": queue_dir,
        "proc_config": app_conf["PROC_CONFIG"],
        "submission_file_types": app_conf["SUBMISSION_FILE_TYPES"],
        "queue": queue
    })
    qw_process.daemon = True
    app.logger.debug("Finished instantiating process")
    # app.logger.debug("Registering exit handler...") #
    # atexit.register(qw_process.join) #
    # app.logger.debug("Finished registering exit handler") #
    # app.logger.debug("Registering signal handler...")
    # print("This is a test message 1")
    #
    # def shutdown_server(signal_, frame):
    #     # qw.done = True
    #     qw_process.terminate()
    #     signal.signal(signal.SIGINT, old_sig_handler)
    #     signal.raise_signal(signal_)
    #     # shutdown_func() *
    #
    # if not created_process:
    #     old_sig_handler = signal.signal(signal.SIGINT, shutdown_server)
    #     created_process = True
    # # shutdown_func = request.environ.get("werkzeug.server.shutdown") *
    # # if shutdown_func is None: *
    # #     raise RuntimeError("Werkzeug server shutdown function not found") *
    # # signal.signal(signal.SIGINT, shutdown_server)
    # print("This is a test message 2")
    # app.logger.debug("Finished registering signal handler")
    app.logger.debug("Starting process...")
    qw_process.start()
    print("This is a test message 3")
    app.logger.info("Finished pre-screening program initialization")
    socket_server_key = {}

    pprinter = pprint.PrettyPrinter()
    # TODO: multiprocessing

    @app.route("/audio", methods=["GET", "POST", "PATCH"])
    def audio_resource():
        """
        GET: Get a batch of at most UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB
        Atlas.

        POST: Submit one or more recordings for pre-screening and upload them to the database if they all pass. The
        arguments for each recording correspond to an index. If providing any values for an argument, represent empty
        arguments with an empty string.

        PATCH: Attach the given arguments to multiple unprocessed audio documents and move them to the Audio collection.
        Additionally, update the recording history of the associated questions and users.
        """
        if request.method == "GET":
            return get_unprocessed_audio()
        elif request.method == "POST":
            decoded = _verify_id_token()
            user_id = decoded['uid']

            recordings = request.files.getlist("audio")
            qb_ids = request.form.getlist("qb_id")
            sentence_ids = request.form.getlist("sentenceId")
            diarization_metadatas = request.form.getlist("diarMetadata")
            rec_types = request.form.getlist("recType")

            if not (len(recordings) == len(rec_types)
                    and (not qb_ids or len(recordings) == len(qb_ids))
                    and (not sentence_ids or len(recordings) == len(sentence_ids))
                    and (not diarization_metadatas or len(recordings) == len(diarization_metadatas))):
                app.logger.error("Received incomplete form batch. Aborting")
                return "incomplete_batch", HTTPStatus.BAD_REQUEST

            # for i in range(len(recordings)):
            #     recording = recordings[i]
            #     rec_type = rec_types[i]
            #     qb_id = qb_ids[i] if len(qb_ids) > i else None
            #     sentence_id = sentence_ids[i] if len(sentence_ids) > i else None
            #     diarization_metadata = diarization_metadata_list[i] if len(diarization_metadata_list) > i else None
            #     result = pre_screen(recording, rec_type, qb_id, sentence_id, user_id, diarization_metadata)
            #
            #     # Stops pre-screening on the first submission that fails. Meant to represent an all-or-nothing logic
            #     # flow, but it isn't really successful.
            #     if result[1] != HTTPStatus.ACCEPTED or not result[0].get("prescreenSuccessful"):
            #         return result
            # return {"prescreenSuccessful": True}, HTTPStatus.ACCEPTED
            return pre_screen(recordings, rec_types, user_id, qb_ids, sentence_ids, diarization_metadatas)
        elif request.method == "PATCH":
            arguments_batch = request.get_json()
            return handle_processing_results(arguments_batch)

    def get_unprocessed_audio():
        """
        Get a batch of at most UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB
        Atlas.

        :return: A dictionary containing an array of dictionaries under the key "results".
        """
        max_docs = app.config["UNPROC_FIND_LIMIT"]
        errs = []
        results_projection = ["_id", "diarMetadata"]  # In addition to the transcript

        app.logger.info(f"Finding a batch ({max_docs} max) of unprocessed audio documents...")
        audio_cursor = qtpm.unproc_audio.find(limit=max_docs)
        id2entries = {}
        qids = []
        audio_doc_count = 0
        for audio_doc in audio_cursor:
            _debug_variable("audio_doc", audio_doc)

            qid = audio_doc.get("qb_id")
            sentence_id = audio_doc.get("sentenceId")
            if qid is None:
                app.logger.warning("Audio document does not contain question ID")
                errs.append(("internal_error", "undefined_qb_id"))
                continue
            id_key = "_".join([str(qid), str(sentence_id)]) if sentence_id else str(qid)
            if id_key not in id2entries:
                id2entries[id_key] = []
            entry = {}
            for field in results_projection:
                if field in audio_doc:
                    entry[field] = audio_doc[field]
            id2entries[id_key].append(entry)
            qids.append(qid)
            audio_doc_count += 1

        if audio_doc_count == 0:
            app.logger.error("Could not find any audio documents")
            return "empty_unproc_audio", HTTPStatus.NOT_FOUND

        _debug_variable("id2entries", id2entries)
        app.logger.info(f"Found {audio_doc_count} unprocessed audio document(s)")
        if not qids:
            app.logger.error("No audio documents contain question IDs")
            return "empty_qid2entries", HTTPStatus.NOT_FOUND

        question_gen = qtpm.find_questions(qids)
        for question in question_gen:
            _debug_variable("question", question)
            qid = question["qb_id"]
            sentence_id = question.get("sentenceId")
            id_key = "_".join([str(qid), str(sentence_id)]) if sentence_id else str(qid)
            transcript = question.get("transcript")
            if transcript:
                entries = id2entries[id_key]
                for entry in entries:
                    entry["transcript"] = transcript

        _debug_variable("id2entries", id2entries)

        results = []
        for entries in id2entries.values():
            results += entries
        app.logger.debug(f"Final Results: {results!r}")
        response = {"results": results}
        if errs:
            response["errors"] = [{"type": err[0], "reason": err[1]} for err in errs]
        return response

    def pre_screen(recordings: List[werkzeug.datastructures.FileStorage],
                   rec_types: List[str],
                   user_id: str,
                   qb_ids: List[Union[int, str]] = None,
                   sentence_ids: List[Union[int, str]] = None,
                   diarization_metadata_list: List[str] = None) -> Tuple[Union[dict, str], int]:
        """
        Submit one or more recordings for pre-screening and upload them to the database if they all pass.
        WARNING: Submitting recordings of different rec_types in the same batch can give unpredictable results.

        :param recordings: A list of audio files
        :param rec_types: A list of rec_types associated with each recording
        :param user_id: The ID of the submitter
        :param qb_ids: A list of question IDs associated with each recording
        :param sentence_ids: The associated sentence IDs. Only required for segmented questions
        :param diarization_metadata_list: (optional) The associated parameter sets for diarization.
        :return: A dictionary with the key "prescreenSuccessful", or a string if an error occurred, and a status code.
        """
        valid_rec_types = ["normal", "buzz", "answer"]

        submission_names = []

        for i in range(len(recordings)):
            recording = recordings[i]
            rec_type = rec_types[i]
            qb_id = qb_ids[i] if len(qb_ids) > i else None
            sentence_id = sentence_ids[i] if len(sentence_ids) > i else None
            diarization_metadata = diarization_metadata_list[i] if len(diarization_metadata_list) > i else None

            _debug_variable("qb_id", qb_id)
            _debug_variable("sentence_id", sentence_id)
            _debug_variable("diarization_metadata", diarization_metadata)
            _debug_variable("rec_type", rec_type)

            if not recording:
                app.logger.error("No audio recording defined. Aborting")
                return "arg_audio_undefined", HTTPStatus.BAD_REQUEST

            if not rec_type:
                app.logger.error("Form argument 'recType' is undefined. Aborting")
                return "arg_recType_undefined", HTTPStatus.BAD_REQUEST
            elif rec_type not in valid_rec_types:
                app.logger.error(f"Invalid rec type '{rec_type!r}'. Aborting")
                return "arg_recType_invalid", HTTPStatus.BAD_REQUEST

            qid_required = rec_type != "buzz"
            if qid_required and not qb_id:
                app.logger.error("Form argument 'qid' expected. Aborting")
                return "arg_qid_undefined", HTTPStatus.BAD_REQUEST

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
            _debug_variable("user_id", user_id)

            metadata = {
                "recType": rec_type,
                "userId": user_id
            }
            if diarization_metadata:
                metadata["diarMetadata"] = diarization_metadata
            if qb_id:
                metadata["qb_id"] = int(qb_id)
            if sentence_id:
                metadata["sentenceId"] = int(sentence_id)
            elif rec_type == "normal" and len(recordings) > 1:
                app.logger.debug("Field 'sentenceId' not specified in batch submission")
                metadata["__sentenceIndex"] = i

            submission_name = _save_recording(queue_dir, recording, metadata)
            _debug_variable("submission_name", submission_name)

            submission_names.append(submission_name)
        # TODO: Handle files being processed in an arbitrary order
        app.logger.info("Awaiting result...")
        pre_screen_successful = True
        while submission_names:
            _debug_variable("submission_names", submission_names)
            result = queue.get(True, 3600)  # Block for at most 1 hour for now.
            app.logger.info("Received submission. Evaluating name...")
            _debug_variable("queue.get()", result)
            if result["name"] in submission_names:
                app.logger.info("Name matches. Removing from list and evaluating if pre-screen is successful")
                submission_names.remove(result["name"])
                if result["case"] == "rejected":
                    app.logger.info("Submission was rejected. Returning result")
                    pre_screen_successful = False
                    break
                app.logger.info("Submission was accepted. Continuing...")
            else:
                # TODO: REALLY consider not doing this in the future.
                app.logger.info("Name does not match. Pushing back onto queue")
                queue.put(result)

        app.logger.info(f"Finished evaluating results. Final Result: {pre_screen_successful}")
        return {"prescreenSuccessful": pre_screen_successful}, HTTPStatus.ACCEPTED

        # try:
        #     results = qp.pick_submissions(
        #         rec_processing.QuizzrWatcher.queue_submissions(
        #             app.config["REC_DIR"],
        #             size_limit=app.config["PROC_CONFIG"]["queueLimit"]
        #         )
        #     )
        # except BrokenPipeError as e:
        #     app.logger.error(f"Encountered BrokenPipeError: {e}. Aborting")
        #     return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR
        #
        # # Split by recType
        # app.logger.info("Preparing results for upload...")
        # file_paths = {}
        # for submission in results:
        #     file_path = os.path.join(app.config["REC_DIR"], submission) + ".wav"
        #     if results[submission]["case"] == "accepted":
        #         sub_rec_type = results[submission]["metadata"]["recType"]
        #         if sub_rec_type not in file_paths:
        #             file_paths[sub_rec_type] = []
        #         file_paths[sub_rec_type].append(file_path)
        #
        # _debug_variable("file_paths", file_paths)
        #
        # # Upload files
        # try:
        #     file2blob = {}
        #     for rt, paths in file_paths.items():  # Organize by recType
        #         file2blob.update(qtpm.upload_many(paths, rt))
        # except BrokenPipeError as e:
        #     app.logger.error(f"Encountered BrokenPipeError: {e}. Aborting")
        #     return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR
        #
        # _debug_variable("file2blob", file2blob)
        #
        # # sub2blob = {os.path.splitext(file)[0]: file2blob[file] for file in file2blob}
        # sub2meta = {}
        # sub2vtt = {}
        #
        # for submission in results:
        #     doc = results[submission]
        #     if doc["case"] == "accepted":
        #         sub2meta[submission] = doc["metadata"]
        #         if "vtt" in doc:
        #             sub2vtt[submission] = doc.get("vtt")
        #
        # # Upload submission metadata to MongoDB
        # qtpm.mongodb_insert_submissions(
        #     sub2blob={os.path.splitext(file)[0]: file2blob[file] for file in file2blob},
        #     sub2meta=sub2meta,
        #     sub2vtt=sub2vtt
        # )
        #
        # app.logger.info("Evaluating outcome of pre-screen...")
        #
        # for submission in results:
        #     app.logger.info(f"Removing submission with name '{submission}'")
        #     rec_processing.delete_submission(app.config["REC_DIR"], submission, app.config["SUBMISSION_FILE_TYPES"])
        #
        # for submission_name in submission_names:
        #     if results[submission_name]["case"] == "rejected":
        #         return {"prescreenSuccessful": False}, HTTPStatus.ACCEPTED
        #
        #     if results[submission_name]["case"] == "err":
        #         return results[submission_name]["err"], HTTPStatus.INTERNAL_SERVER_ERROR
        #
        # return {"prescreenSuccessful": True}, HTTPStatus.ACCEPTED

    def handle_processing_results(arguments_batch: Dict[str, List[Dict[str, Any]]]):
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

        question = qtpm.rec_questions.find_one({"qb_id": int(qid)})
        if not question:
            return "question_not_found", HTTPStatus.NOT_FOUND
        correct_answer = question.get("answer")
        if not correct_answer:
            return "answer_not_found", HTTPStatus.NOT_FOUND

        answer_similarity = fuzz.token_set_ratio(user_answer, correct_answer)
        _debug_variable("answer_similarity", answer_similarity)
        return {"correct": answer_similarity >= app.config["MIN_ANSWER_SIMILARITY"]}

    @app.route("/audio/<path:blob_path>", methods=["GET"])
    def retrieve_audio_file(blob_path):
        """Retrieve a file from Firebase Storage."""
        return send_file(qtpm.get_file_blob(blob_path), mimetype="audio/wav")

    @app.post("/socket/key")
    def generate_game_key():
        """Generate a secret key to represent the socket server.

        WARNING: A secret key can be generated only once per server session. This key cannot be recovered if lost."""
        if socket_server_key:
            app.logger.error("Secret key for socket server already exists. Aborting")
            return "secret_key_exists", HTTPStatus.UNAUTHORIZED
        app.logger.info("Generating secret key for socket server...")
        key = token_urlsafe(256)
        socket_server_key["value"] = key
        app.logger.info("Successfully generated secret key for socket server")
        return {"key": key}

    @app.put("/game_results")
    def handle_game_results():
        """
        Update the database with the results of a game session.
        Precondition: session_results["questionStats"]["played"] != 0
        """
        secret_key_arg = request.headers.get("Authorization")
        _debug_variable("secret_key_arg", secret_key_arg, private=True)
        if secret_key_arg != socket_server_key["value"]:
            app.logger.info("Secret key does not match. Access to resource denied")
            abort(HTTPStatus.UNAUTHORIZED)
        app.logger.info("Access to resource granted")
        session_results = request.get_json()
        _debug_variable("session_results", session_results)
        if "category" in session_results:
            return handle_game_results_category(session_results)
        if "categories" in session_results:
            return handle_game_results_categories(session_results)
        return {
            "err": "Arguments 'category' or 'categories' not provided",
            "err_id": "undefined_args",
            "extra": ["category_or_categories"]
        }, HTTPStatus.BAD_REQUEST

    def handle_game_results_category(session_results):
        mode = session_results["mode"]
        category = session_results["category"]
        user_results = session_results["users"]
        update_batch = []
        app.logger.info(f"Processing updates for {len(session_results['users'])} users...")
        for username, update_args in user_results.items():
            app.logger.info(f"Finding user profile with username '{username}'...")
            user_doc = qtpm.users.find_one({"username": username})
            if "stats" not in user_doc:
                app.logger.debug("Creating stub for field 'stats'...")
                user_doc["stats"] = {}

            if mode not in user_doc["stats"]:
                app.logger.debug(f"Creating stub for mode '{mode}'...")
                user_doc["stats"][mode] = {
                    "questions": {
                        "played": {"all": 0},
                        "buzzed": {"all": 0},
                        "correct": {"all": 0},
                        "cumulativeProgressOnBuzz": {},
                        "avgProgressOnBuzz": {}
                    },
                    "game": {
                        "played": {"all": 0},
                        "won": {"all": 0}
                    }
                }

            _debug_variable(f"user.stats.{mode}", user_doc["stats"][mode])

            old_question_stats = user_doc["stats"][mode]["questions"]

            for field in ["played", "buzzed", "correct"]:
                if category not in old_question_stats[field]:
                    app.logger.debug(f"Set default value for field '{field}', category '{category}'")
                    old_question_stats[field][category] = 0

            old_game_stats = user_doc["stats"][mode]["game"]

            for field in ["played", "won"]:
                if category not in old_game_stats[field]:
                    app.logger.debug(f"Set default value for field '{field}', category '{category}'")
                    old_game_stats[field][category] = 0

            question_stats = update_args["questionStats"]

            app.logger.info("Retrieving projected results...")

            played_all = old_question_stats["played"]["all"] + question_stats["played"]
            played_categorical = old_question_stats["played"][category] + question_stats["played"]
            buzzed_all = old_question_stats["buzzed"]["all"] + question_stats["buzzed"]
            buzzed_categorical = old_question_stats["buzzed"][category] + question_stats["buzzed"]

            _debug_variable("total_questions_played", played_all)
            _debug_variable("total_buzzes", buzzed_all)
            _debug_variable("categorical_questions_played", played_categorical)
            _debug_variable("categorical_buzzes", buzzed_categorical)

            app.logger.info("Projected results retrieved")

            old_c_progress_on_buzz = old_question_stats["cumulativeProgressOnBuzz"]
            c_progress_on_buzz = question_stats["cumulativeProgressOnBuzz"]

            old_avg_progress_on_buzz = old_question_stats["avgProgressOnBuzz"]

            _debug_variable("old_avg_progress_on_buzz", old_avg_progress_on_buzz)
            app.logger.info("Calculating derived statistics...")

            avg_progress_on_buzz_update = {}
            for k in c_progress_on_buzz.keys():
                if k not in old_c_progress_on_buzz:
                    old_c_progress_on_buzz[k] = {"all": 0}

                if category not in old_c_progress_on_buzz[k]:
                    old_c_progress_on_buzz[k][category] = 0

                avg_progress_on_buzz_update[k] = {
                    "all": (old_c_progress_on_buzz[k]["all"] + c_progress_on_buzz[k]) / played_all,
                    category: (old_c_progress_on_buzz[k][category] + c_progress_on_buzz[k]) / played_categorical
                }

            avg_progress_on_buzz = deepcopy(old_avg_progress_on_buzz)
            sv_util.deep_update(avg_progress_on_buzz, avg_progress_on_buzz_update)

            _debug_variable("avg_progress_on_buzz", avg_progress_on_buzz)

            buzz_rate = {
                "all": buzzed_all / played_all,
                category: buzzed_categorical / played_categorical
            }

            _debug_variable("buzz_rate.all", buzz_rate["all"])
            _debug_variable(f"buzz_rate.{category}", buzz_rate[category])

            buzz_accuracy = {}
            if buzzed_all == 0:
                app.logger.debug("Number of total buzz-ins is 0. Skipping accuracy calculation")
                buzz_accuracy["all"] = None
            else:
                buzz_accuracy["all"] = (old_question_stats["correct"]["all"] + question_stats["correct"]) / buzzed_all
                _debug_variable("buzz_accuracy.all", buzz_accuracy["all"])
            if buzzed_categorical == 0:
                app.logger.debug("Number of categorical buzz-ins is 0. Skipping accuracy calculation")
                buzz_accuracy[category] = None
            else:
                buzz_accuracy[category] = (old_question_stats["correct"][category] + question_stats["correct"]) \
                                          / buzzed_categorical
                _debug_variable(f"buzz_accuracy.{category}", buzz_accuracy[category])

            win_rate = {
                "all": (old_game_stats["won"]["all"] + int(update_args["won"]))
                / (old_game_stats["played"]["all"] + 1),
                category: (old_game_stats["won"][category] + int(update_args["won"]))
                / (old_game_stats["played"][category] + 1)
            }

            _debug_variable(f"win_rate.all", win_rate["all"])
            _debug_variable(f"win_rate.{category}", win_rate[category])

            app.logger.info("Calculated derived statistics")

            stats_index = f"stats.{mode}"
            q_index = f"{stats_index}.questions"
            g_index = f"{stats_index}.game"

            processed_update_args = {
                "$inc": {
                    f"{q_index}.played.all": question_stats["played"],
                    f"{q_index}.buzzed.all": question_stats["buzzed"],
                    f"{q_index}.correct.all": question_stats["correct"],
                    f"{q_index}.cumulativeProgressOnBuzz.percentQuestionRead.all": c_progress_on_buzz[
                        "percentQuestionRead"],
                    f"{q_index}.cumulativeProgressOnBuzz.numSentences.all": c_progress_on_buzz["numSentences"],

                    f"{g_index}.played.all": 1,
                    f"{g_index}.finished.all": int(update_args["finished"]),
                    f"{g_index}.won.all": int(update_args["won"]),

                    f"{q_index}.played.{category}": question_stats["played"],
                    f"{q_index}.buzzed.{category}": question_stats["buzzed"],
                    f"{q_index}.correct.{category}": question_stats["correct"],
                    f"{q_index}.cumulativeProgressOnBuzz.percentQuestionRead.{category}": c_progress_on_buzz[
                        "percentQuestionRead"],
                    f"{q_index}.cumulativeProgressOnBuzz.numSentences.{category}": c_progress_on_buzz["numSentences"],

                    f"{g_index}.played.{category}": 1,
                    f"{g_index}.finished.{category}": int(update_args["finished"]),
                    f"{g_index}.won.{category}": int(update_args["won"])
                },
                "$set": {
                    f"{q_index}.buzzRate.all": buzz_rate["all"],
                    f"{q_index}.buzzRate.{category}": buzz_rate[category],

                    f"{q_index}.buzzAccuracy.all": buzz_accuracy["all"],
                    f"{q_index}.buzzAccuracy.{category}": buzz_accuracy[category],

                    f"{q_index}.avgProgressOnBuzz": avg_progress_on_buzz,

                    f"{g_index}.winRate.all": win_rate["all"],
                    f"{g_index}.winRate.{category}": win_rate[category]
                }
            }
            update_batch.append(UpdateOne({"username": username}, processed_update_args))
        app.logger.info("Sending bulk write operation...")
        results = qtpm.users.bulk_write(update_batch)
        app.logger.info(f"Matched {results.matched_count} documents and modified {results.modified_count} documents")
        app.logger.info(f"Request body contained profile updates for {len(session_results['users'])} users")
        return {"successful": results.matched_count, "requested": len(session_results["users"])}

    def handle_game_results_categories(session_results):
        mode = session_results["mode"]
        categories = session_results["categories"]
        user_results = session_results["users"]
        update_batch = []

        for username, update_args in user_results.items():
            app.logger.info(f"Finding user profile with username '{username}'...")
            user_doc = qtpm.users.find_one({"username": username})

            if "stats" not in user_doc:
                app.logger.debug("Creating stub for field 'stats'...")
                user_doc["stats"] = {}

            if mode not in user_doc["stats"]:
                app.logger.debug(f"Creating stub for mode '{mode}'...")
                user_doc["stats"][mode] = {
                    "questions": {
                        "played": {"all": 0},
                        "buzzed": {"all": 0},
                        "correct": {"all": 0},
                        "cumulativeProgressOnBuzz": {},
                        "avgProgressOnBuzz": {}
                    },
                    "game": {
                        "played": {"all": 0},
                        "won": {"all": 0}
                    }
                }

            _debug_variable(f"user.stats.{mode}", user_doc["stats"][mode])

            old_question_stats = user_doc["stats"][mode]["questions"]
            old_game_stats = user_doc["stats"][mode]["game"]
            _debug_variable("old_question_stats", old_question_stats)
            _debug_variable("old_game_stats", old_game_stats)

            for field in ["played", "won"]:
                for category in categories:
                    if category not in old_game_stats[field]:
                        app.logger.debug(f"Set default value for field '{field}', category '{category}' in game stats")
                        old_game_stats[field][category] = 0

            question_stats = update_args["questionStats"]

            for field in ["played", "buzzed", "correct"]:
                for category in question_stats[field]:
                    if category not in old_question_stats[field]:
                        app.logger.debug(f"Set default value for field '{field}', category '{category}', in question "
                                         "stats")
                        old_question_stats[field][category] = 0

            stats_index = f"stats.{mode}"
            q_index = f"{stats_index}.questions"
            g_index = f"{stats_index}.game"

            processed_update_args = {"$inc": {}, "$set": {}}
            # Get totals for the played, buzzed, and correct fields while also adding $inc arguments.
            totals = {}
            for field in ["played", "buzzed", "correct"]:
                app.logger.info(f"Adding incrementation arguments for field '{field}' in question stats...")
                total_value = 0
                for category, value in question_stats[field].items():
                    _debug_variable("category", category)
                    field_path = f"{q_index}.{field}.{category}"
                    processed_update_args["$inc"][field_path] = value
                    total_value += value
                processed_update_args["$inc"][f"{q_index}.{field}.all"] = total_value
                totals[field] = total_value
                app.logger.info(f"Successfully added incrementation arguments for field '{field}' in question stats")

            app.logger.info("Adding incrementation arguments for fields in game stats...")
            for category in categories:
                _debug_variable("category", category)
                processed_update_args["$inc"][f"{g_index}.played.{category}"] = 1
                processed_update_args["$inc"][f"{g_index}.finished.{category}"] = int(update_args["finished"])
                processed_update_args["$inc"][f"{g_index}.won.{category}"] = int(update_args["won"])

            processed_update_args["$inc"][f"{g_index}.played.all"] = 1
            processed_update_args["$inc"][f"{g_index}.finished.all"] = int(update_args["finished"])
            processed_update_args["$inc"][f"{g_index}.won.all"] = int(update_args["won"])
            app.logger.info("Successfully added incrementation arguments for fields in game stats")

            # Add to cumulativeProgressOnBuzz fields and calculate avgProgressOnBuzz fields
            app.logger.info("Calculating average stats on buzz timings...")
            for field, categorical_values in question_stats["cumulativeProgressOnBuzz"].items():
                _debug_variable(f"cumulativeProgressOnBuzz.{field}", categorical_values)
                total_value = 0
                if field not in old_question_stats["cumulativeProgressOnBuzz"]:
                    old_question_stats["cumulativeProgressOnBuzz"][field] = {}

                old_categorical_values = old_question_stats["cumulativeProgressOnBuzz"][field]
                _debug_variable("old_categorical_values", old_categorical_values)

                for category, value in categorical_values.items():
                    if category not in old_categorical_values:
                        old_categorical_values[category] = 0
                    field_path = f"{q_index}.cumulativeProgressOnBuzz.{field}.{category}"
                    processed_update_args["$inc"][field_path] = value
                    avg_field_path = f"{q_index}.avgProgressOnBuzz.{field}.{category}"
                    projected_value = value + old_categorical_values[category]
                    projected_n_played = old_question_stats["played"][category] + question_stats["played"][category]
                    if projected_n_played == 0:
                        projected_average = None
                    else:
                        projected_average = projected_value / projected_n_played
                    processed_update_args["$set"][avg_field_path] = projected_average
                    total_value += value

                if "all" not in old_categorical_values:
                    old_categorical_values["all"] = 0
                processed_update_args["$inc"][f"{q_index}.cumulativeProgressOnBuzz.{field}.all"] = total_value
                total_projected_value = total_value + old_categorical_values["all"]
                total_projected_n_played = old_question_stats["played"]["all"] + totals["played"]
                if total_projected_n_played == 0:
                    total_projected_average = None
                else:
                    total_projected_average = total_projected_value / total_projected_n_played
                processed_update_args["$set"][f"{q_index}.avgProgressOnBuzz.{field}.all"] = total_projected_average
            app.logger.info("Successfully calculated average stats on buzz timings")

            # Calculate derived question statistics
            app.logger.info("Calculating derived question statistics...")
            for category in question_stats["played"]:
                projected_played = old_question_stats["played"][category] + question_stats["played"][category]
                projected_buzzed = old_question_stats["buzzed"][category] + question_stats["buzzed"][category]
                projected_correct = old_question_stats["correct"][category] + question_stats["correct"][category]

                if projected_played == 0:
                    buzz_rate = None
                else:
                    buzz_rate = projected_buzzed / projected_played

                if projected_buzzed == 0:
                    buzz_accuracy = None
                else:
                    buzz_accuracy = projected_correct / projected_buzzed

                processed_update_args["$set"][f"{q_index}.buzzRate.{category}"] = buzz_rate
                processed_update_args["$set"][f"{q_index}.buzzAccuracy.{category}"] = buzz_accuracy

            projected_played = old_question_stats["played"]["all"] + totals["played"]
            projected_buzzed = old_question_stats["buzzed"]["all"] + totals["buzzed"]
            projected_correct = old_question_stats["correct"]["all"] + totals["correct"]

            if projected_played == 0:
                buzz_rate = None
            else:
                buzz_rate = projected_buzzed / projected_played

            if projected_buzzed == 0:
                buzz_accuracy = None
            else:
                buzz_accuracy = projected_correct / projected_buzzed

            processed_update_args["$set"][f"{q_index}.buzzRate.all"] = buzz_rate
            processed_update_args["$set"][f"{q_index}.buzzAccuracy.all"] = buzz_accuracy
            app.logger.info("Successfully calculated derived question statistics")

            # Calculate win rate for each category
            app.logger.info("Calculating win rates...")
            for category in categories:
                projected_won = old_game_stats["won"][category] + int(update_args["won"])
                projected_played = old_game_stats["played"][category] + 1
                win_rate = projected_won / projected_played
                processed_update_args["$set"][f"{g_index}.winRate.{category}"] = win_rate

            projected_won = old_game_stats["won"]["all"] + int(update_args["won"])
            projected_played = old_game_stats["played"]["all"] + 1
            win_rate = projected_won / projected_played
            processed_update_args["$set"][f"{g_index}.winRate.all"] = win_rate
            app.logger.info("Successfully calculated win rates")

            _debug_variable(f"processed_update_args.{username}", processed_update_args)

            update_batch.append(UpdateOne({"username": username}, processed_update_args))
        _debug_variable("update_batch", update_batch)
        app.logger.info("Sending bulk write operation...")
        results = qtpm.users.bulk_write(update_batch)
        app.logger.info(f"Matched {results.matched_count} documents and modified {results.modified_count} documents")
        app.logger.info(f"Request body contained profile updates for {len(session_results['users'])} users")
        return {"successful": results.matched_count, "requested": len(session_results["users"])}

    @app.route("/leaderboard", methods=["GET"])
    def get_leaderboard():
        category = request.args.get("category") or "all"
        arg_size = request.args.get("size")
        size = arg_size or app.config["DEFAULT_LEADERBOARD_SIZE"]
        if size > app.config["MAX_LEADERBOARD_SIZE"]:
            return {
                "err": f"Given size exceeds allowable limit ({app.config['MAX_LEADERBOARD_SIZE']})",
                "err_id": "invalid_arg",
                "extra": ["exceeds_value", app.config["MAX_LEADERBOARD_SIZE"]]
            }, HTTPStatus.BAD_REQUEST
        visibility_config = app.config["VISIBILITY_CONFIGS"]["basic"]
        cursor = qtpm.database.get_collection(visibility_config["collection"]).find(
            {f"ratings.{category}": {"$exists": True}},
            sort=[(f"ratings.{category}", pymongo.DESCENDING)],
            limit=size,
            projection=visibility_config["projection"]
        )
        return {"results": [doc for doc in cursor]}

    @app.route("/profile", methods=["GET", "POST", "PATCH", "DELETE"])
    def own_profile():
        """A resource to automatically point to the user's own profile."""
        decoded = _verify_id_token()
        # decoded = {"uid": "dev"}
        user_id = decoded["uid"]
        if request.method == "GET":
            result = qtpm.get_profile(user_id, "private")
            if result is None:
                app.logger.error(f"User with ID '{user_id}' does not have a profile. Aborting")
                abort(HTTPStatus.NOT_FOUND)
            return result
        elif request.method == "POST":
            args = request.get_json()
            path, op = api.path_for("create_profile")
            schema = api.build_schema(api.api["paths"][path][op]["requestBody"]["content"]["application/json"]["schema"])
            err = _validate_args(args, schema)
            if err:
                return err
            try:
                result = qtpm.create_profile(user_id, args["pfp"], args["username"])
            except pymongo.errors.DuplicateKeyError:
                app.logger.error("User already registered. Aborting")
                return "already_registered", HTTPStatus.BAD_REQUEST
            except UsernameTakenError as e:
                app.logger.error(f"User already exists: {e}. Aborting")
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

            try:
                result = qtpm.modify_profile(user_id, update_args)
            except UsernameTakenError as e:
                app.logger.error(f"Username already taken: {e}. Aborting")
                return f"username_taken: {e}", HTTPStatus.BAD_REQUEST

            if result:
                app.logger.info("User profile successfully modified")
                return "user_modified", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR
        elif request.method == "DELETE":
            result = qtpm.delete_profile(user_id)
            if result:
                app.logger.info("User profile successfully deleted")
                return "user_deleted", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR

    @app.route("/profile/<username>", methods=["GET", "PATCH", "DELETE"])
    def other_profile(username):
        """Resource for the profile of any user."""
        _debug_variable("username", username)
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
                app.logger.info("User profile successfully modified")
                return "other_user_modified", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR
        elif request.method == "DELETE":
            result = qtpm.delete_profile(other_user_id)
            if result:
                app.logger.info("User profile successfully deleted")
                return "other_user_deleted", HTTPStatus.OK
            return "unknown_error", HTTPStatus.INTERNAL_SERVER_ERROR

    @app.route("/question", methods=["GET"])
    def pick_game_question():
        """Retrieve a batch of randomly-selected questions and attempt to retrieve the associated recordings with the
        best evaluations possible without getting recordings from different users in the same question."""
        # cursor = qtpm.rec_questions.find({"qb_id": {"$exists": True}}, {"qb_id": 1})
        # question_ids = list({doc["qb_id"] for doc in cursor})  # Ensure no duplicates are present
        # if not question_ids:
        #     app.logger.error("No recorded questions found. Aborting")
        #     return "rec_empty_qids", HTTPStatus.NOT_FOUND
        #
        # # question_ids = qtpm.rec_question_ids.copy()
        # audios = []
        # while True:
        #     next_question_id = random.choice(question_ids)
        #     cursor = qtpm.rec_questions.find({"qb_id": next_question_id})
        #     app.logger.debug(f"{type(next_question_id)} next_question_id = {next_question_id!r}")
        #
        #     all_valid = True
        #     for sentence in cursor:
        #         if sentence and sentence.get("recordings"):
        #             app.logger.debug(f"sentence = {sentence!r}")
        #             audio = qtpm.find_best_audio_doc(
        #                 sentence.get("recordings"),
        #                 required_fields=["_id", "questionId", "sentenceId", "vtt", "gentleVtt"]
        #             )
        #             if not audio:
        #                 all_valid = False
        #                 break
        #             audios.append(audio)
        #     if all_valid:
        #         break
        #     app.logger.warning(
        #         f"ID {next_question_id} is invalid or associated question has no valid audio recordings")
        #     question_ids.remove(next_question_id)
        #     if not question_ids:
        #         app.logger.error("Failed to find a viable recorded question. Aborting")
        #         return "rec_corrupt_questions", HTTPStatus.NOT_FOUND
        # return {"results": audios}
        categories = request.args.getlist("category")
        difficulty_range_arg = request.args.get("difficultyRange")
        batch_size = int(request.args.get("batchSize") or 1)
        pipeline = [
            {'$group': {'_id': '$qb_id', 'category': {'$first': '$category'}}},
            {'$sample': {'size': batch_size}},
            {'$lookup': {
                'from': 'Audio',
                'as': 'audio',
                'let': {'qb_id': '$_id'},
                'pipeline': [
                    {'$match': {'$expr': {'$eq': ['$qb_id', '$$qb_id']}}},
                    {'$set': {'totalScore': {'$add': ['$score.wer', '$score.mer', '$score.wil']}}},
                    {'$sort': {'totalScore': 1}},
                    {'$group': {
                        '_id': {'userId': '$userId', 'sentenceId': '$sentenceId'},
                        'id': {'$first': '$_id'},
                        'vtt': {'$first': '$vtt'},
                        'gentleVtt': {'$first': '$gentleVtt'},
                        'lowestScore': {'$first': '$totalScore'}
                    }},
                    {'$group': {
                        '_id': '$_id.userId',
                        'netScore': {'$sum': '$lowestScore'},
                        'audio': {'$push': {
                            'id': '$id',
                            'sentenceId': '$_id.sentenceId',
                            'vtt': '$vtt',
                            'gentleVtt': '$gentleVtt'
                        }}
                    }},
                    {'$sort': {'netScore': 1}},
                    {'$limit': 1},
                    {'$unwind': {'path': '$audio'}},
                    {'$replaceRoot': {'newRoot': '$audio'}}
                ]
            }}
        ]
        query = {}
        if categories:
            query['category'] = {'$in': categories}

        if difficulty_range_arg:
            difficulty_range = [int(num) for num in re.split(r",\s*", difficulty_range_arg)]
            query['difficultyNum'] = {'$gte': difficulty_range[0], '$lte': difficulty_range[1]}

        if query:
            pipeline.insert(0, {'$match': query})
        cursor = qtpm.rec_questions.aggregate(pipeline)
        questions = []
        for doc in cursor:
            doc["qb_id"] = doc.pop("_id")
            questions.append(doc)
        return {"results": questions}

    @app.route("/question/unrec", methods=["GET", "POST"])
    def unrec_question_resource():
        if request.method == "GET":
            difficulty = request.args.get("difficultyType")
            batch_size = request.args.get("batchSize")
            return pick_recording_question(int(difficulty) if difficulty else difficulty, int(batch_size or 1))
        elif request.method == "POST":
            arguments_batch = request.get_json()
            return upload_questions(arguments_batch)

    def pick_recording_question(difficulty: int, batch_size: int):
        """
        Find a random unrecorded question (or multiple) and return the ID and transcript.

        :param difficulty: The difficulty type to use
        :param batch_size: The number of questions to retrieve
        :return: A dictionary containing a list of "results" and "errors"
        """
        if difficulty is not None:
            difficulty_query_op = QuizzrTPM.get_difficulty_query_op(app.config["DIFFICULTY_LIMITS"], difficulty)
            difficulty_query = {"recDifficulty": difficulty_query_op}
        else:
            difficulty_query = {}

        query = {**difficulty_query, "qb_id": {"$exists": True}}

        cursor = qtpm.unrec_questions.find(query, {"qb_id": 1})
        question_ids = list({doc["qb_id"] for doc in cursor})  # Ensure no duplicates are present
        if not question_ids:
            app.logger.error(f"No unrecorded questions found for difficulty type '{difficulty}'. Aborting")
            return "unrec_empty_qids", HTTPStatus.NOT_FOUND

        next_questions, errors = qtpm.pick_random_questions("UnrecordedQuestions", question_ids,
                                                            ["transcript", "tokenizations"], batch_size)
        if next_questions is None:
            return "unrec_corrupt_questions", HTTPStatus.NOT_FOUND
        results = []
        for doc in next_questions:
            result_doc = {"id": doc["qb_id"], "transcript": doc["transcript"], "tokenizations": doc["tokenizations"]}
            if "sentenceId" in doc:
                result_doc["sentenceId"] = doc["sentenceId"]
            results.append(result_doc)
        return {"results": results, "errors": errors}

    def upload_questions(arguments_batch: Dict[str, List[dict]]) -> dict:
        """
        Upload a batch of unrecorded questions.

        :param arguments_batch: A dictionary containing the list of "arguments"
        :return: A "msg" stating that the upload was successful if nothing went wrong
        """
        arguments_list = arguments_batch["arguments"]
        _debug_variable("arguments_list", arguments_list)

        app.logger.info(f"Uploading {len(arguments_list)} unrecorded question(s)...")
        results = qtpm.unrec_questions.insert_many(arguments_list)
        app.logger.info(f"Successfully uploaded {len(results.inserted_ids)} question(s)")
        return {"msg": "unrec_question.upload_success"}

    @app.route("/validate", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"])
    def check_token():
        """Endpoint for ORY Oathkeeper to validate a Bearer token. Accepts every standard method type because
        Oathkeeper also forwards the method type."""
        decoded = _verify_id_token()
        user_id = decoded["uid"]
        try:
            subject = qtpm.get_user_role(user_id)
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

    def _save_recording(directory: str, recording: werkzeug.datastructures.FileStorage, metadata: dict):
        """Write a WAV file and its JSON metadata to disk."""
        app.logger.info("Saving recording...")
        submission_name = _get_next_submission_name()
        _debug_variable("submission_name", submission_name)
        submission_path = os.path.join(directory, submission_name)
        recording.save(submission_path + ".wav")
        app.logger.info("Saved recording successfully")

        app.logger.info("Writing metadata...")
        _debug_variable("metadata", metadata)
        with open(submission_path + ".json", "w") as transaction_f:
            transaction_f.write(bson.json_util.dumps(metadata))
        app.logger.info("Successfully wrote metadata")
        return submission_name

    def _verify_id_token():
        """Try to decode the token if provided. If in a production environment, forbid access when the decode fails.
        This function is only callable in the context of a view function."""
        # return {"uid": app.config["DEV_UID"]}
        app.logger.info("Retrieving ID token from header 'Authorization'...")
        id_token = request.headers.get("Authorization")

        _debug_variable("id_token", id_token, private=True)

        if app.config["TESTING"] and not app.config["USE_ID_TOKENS"] and id_token:
            return {"uid": id_token}

        if not id_token:
            if app.config["Q_ENV"] == PROD_ENV_NAME:
                app.logger.error("ID token not found. Aborting")
                abort(HTTPStatus.UNAUTHORIZED)
            else:
                app.logger.warning(f"ID token not found. Authenticated user with default ID '{app.config['DEV_UID']}'")
                return {"uid": app.config["DEV_UID"]}

        # Cut off prefix if it is present
        prefix = "Bearer "
        if id_token.startswith(prefix):
            id_token = id_token[len(prefix):]

        try:
            app.logger.info("Decoding token...")
            decoded = auth.verify_id_token(id_token)
        except (auth.InvalidIdTokenError, auth.ExpiredIdTokenError) as e:
            app.logger.error(f"ID token error encountered: {e!r}. Aborting")
            decoded = None
            abort(HTTPStatus.UNAUTHORIZED)
        except auth.CertificateFetchError:
            app.logger.error("Could not fetch certificate. Aborting")
            decoded = None
            abort(HTTPStatus.INTERNAL_SERVER_ERROR)

        return decoded

    def _get_private_data_string(original):
        """
        Utility function for redacting private data if 'LOG_PRIVATE_DATA' is not True

        :param original: The original value
        :return: The original value if "LOG_PRIVATE_DATA" is True, otherwise "[REDACTED]"
        """
        if app.config["LOG_PRIVATE_DATA"]:
            return original
        return "[REDACTED]"

    def _query_flag(flag: str) -> bool:
        """
        Detect if a query 'flag' is present (e.g., "example.com/foo?bar" vs "example.com/foo"). Only callable in the
        context of a Flask view function.

        :param flag: The name of the flag
        :return: Whether the flag is present
        """
        return request.args.get(flag) is not None

    def _validate_args(args: dict, schema: dict) -> Optional[Tuple[str, int]]:
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

    def _debug_variable(name: str, v, include_type=False, private=False):
        """
        Send a variable's name and value to the given logger.

        :param name: The name of the variable to display in the logger
        :param v: The value of the variable
        :param include_type: Whether to include the type() of the variable
        :param private: Whether to redact the value when configured
        """
        if private:
            val = _get_private_data_string(v)
        else:
            val = v
        if include_type:
            prefix = f"{type(v)} "
        else:
            prefix = ""
        val_pp = pprinter.pformat(val)
        app.logger.debug(f"{prefix}{name} = {val_pp}")

    return app
