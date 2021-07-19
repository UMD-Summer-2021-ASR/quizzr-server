import json
import logging
import os
import random
from copy import deepcopy
from datetime import datetime
from http import HTTPStatus

import bson.json_util
from flask import Flask, request, render_template, send_file
from flask_cors import CORS
from werkzeug.exceptions import abort

import error_handling
import rec_processing
from tpm import QuizzrTPM

logging.basicConfig(level=os.environ.get("QUIZZR_LOG") or "DEBUG")
logger = logging.getLogger(__name__)


# Flask app factory function
def create_app(test_overrides=None):
    server_dir = os.path.dirname(__file__)
    rec_dir = os.path.join(server_dir, "recordings")
    conf_path_default = os.path.join(server_dir, "sv_config_default.json")
    conf_path = os.path.join(server_dir, "sv_config.json")
    with open(conf_path_default, "r") as default_config_f, open(conf_path, "r") as config_f:
        default_config = json.load(default_config_f)
        config = json.load(config_f)

    app_conf = deepcopy(default_config)
    app_conf.update(config)
    for var in app_conf:
        if type(app_conf[var]) is not str and var in os.environ:
            raise Exception(f"Cannot override non-string config field '{var}' through environment variable")
    app_conf.update(os.environ)
    app_conf["SERVER_DIR"] = server_dir
    app_conf["REC_DIR"] = rec_dir
    if test_overrides:
        app_conf.update(test_overrides)

    app = Flask(__name__)
    app.config.from_mapping(app_conf)
    CORS(app)
    qtpm = QuizzrTPM(app_conf["DATABASE"], app_conf["G_FOLDER"], app_conf["G_DIR_STRUCT"], app_conf["VERSION"])
    qp = rec_processing.QuizzrProcessor(qtpm.database, rec_dir, app_conf["VERSION"], qtpm.gdrive)
    # TODO: multiprocessing

    # Find a random recorded question and return the VTT and ID of the recording with the best evaluation.
    @app.route("/answer/", methods=["GET"])
    def pick_game_question():
        question_ids = QuizzrTPM.get_ids(qtpm.rec_questions)
        if not question_ids:
            logging.error("No recorded questions found. Aborting")
            return "rec_empty_qids", HTTPStatus.NOT_FOUND

        # question_ids = qtpm.rec_question_ids.copy()
        while True:
            next_question_id = random.choice(question_ids)
            next_question = qtpm.rec_questions.find_one({"_id": next_question_id})
            logging.debug(f"{type(next_question_id)} next_question_id = {next_question_id!r}")
            logging.debug(f"next_question = {next_question!r}")

            if next_question and next_question.get("recordings"):
                audio = qtpm.find_best_audio_doc(
                    next_question.get("recordings"),
                    required_fields=["_id", "vtt", "gentleVtt"]
                )
                if audio:
                    break
            logging.warning(f"ID {next_question_id} is invalid or associated question has no valid audio recordings")
            question_ids.remove(next_question_id)
            if not question_ids:
                logging.error("Failed to find a viable recorded question. Aborting")
                return "rec_corrupt_questions", HTTPStatus.NOT_FOUND
        return audio

    # Get a batch of at most UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB Atlas.
    @app.route("/audio/unprocessed/", methods=["GET"])
    def get_unprocessed_audio():
        max_docs = app.config["UNPROC_FIND_LIMIT"]
        errs = []
        results_projection = ["_id", "diarMetadata"]  # In addition to the transcript

        logging.info(f"Finding a batch ({max_docs} max) of unprocessed audio documents...")
        audio_doc_count = qtpm.unproc_audio.count_documents({"_id": {"$exists": True}}, limit=max_docs)
        if audio_doc_count == 0:
            logging.error("Could not find any audio documents")
            return "empty_unproc_audio", HTTPStatus.NOT_FOUND
        audio_cursor = qtpm.unproc_audio.find(limit=max_docs)
        qid2entries = {}
        for audio_doc in audio_cursor:
            logging.debug(f"audio_doc = {audio_doc!r}")

            qid = audio_doc.get("questionId")
            if qid is None:
                logging.warning("Audio document does not contain question ID")
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
        logging.debug(f"qid2entries = {qid2entries!r}")
        logging.info(f"Found {audio_doc_count} unprocessed audio document(s)")
        if not qids:
            logging.error("No audio documents contain question IDs")
            return "empty_qid2entries", HTTPStatus.NOT_FOUND

        questions = qtpm.find_questions(qids)
        for question in questions:
            qid = question["_id"]
            transcript = question.get("transcript")
            if transcript:
                entries = qid2entries[qid]
                for entry in entries:
                    entry["transcript"] = transcript

        logging.debug(f"qid2entries = {qid2entries!r}")

        results = []
        for entries in qid2entries.values():
            results += entries
        logging.debug(f"Final Results: {results!r}")
        response = {"results": results}
        if errs:
            response["errors"] = [{"type": err[0], "reason": err[1]} for err in errs]
        return response

    # Attach the given arguments to multiple unprocessed audio documents and move them to the Audio collection.
    # Additionally, update the recording history of the associated questions and users.
    @app.route("/audio/processed", methods=["POST"])
    def handle_processing_results():
        arguments_batch = request.get_json()
        arguments_list = arguments_batch.get("arguments")
        if arguments_list is None:
            return "undefined_arguments", HTTPStatus.BAD_REQUEST
        logging.info(f"Updating data related to {len(arguments_list)} audio documents...")
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
        logging.info(f"Successfully updated data related to {success_count} of {len(arguments_list)} audio documents")
        logging.info(f"Logged {len(errors)} warning messages")
        return results

    # Check if an answer is correct.
    @app.route("/answer/check", methods=["GET"])
    def check_answer():
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

        return {"correct": user_answer == correct_answer}

    # Retrieve a file from Google Drive.
    @app.route("/download/<gfile_id>", methods=["GET"])
    def retrieve_audio_file(gfile_id):
        if qtpm.gdrive.creds.expired:
            qtpm.gdrive.refresh()
        try:
            file_data = qtpm.get_gfile(gfile_id)
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR
        return send_file(file_data, mimetype="audio/wav")

    # Find a random unrecorded question (or multiple) and return the ID and transcript.
    @app.route("/record/", methods=["GET"])
    def pick_recording_question():
        difficulty = request.args.get("difficultyType")
        batch_size = request.args.get("batchSize")
        if difficulty is not None:
            difficulty_query_op = QuizzrTPM.get_difficulty_query_op(app.config["DIFFICULTY_LIMITS"], int(difficulty))
            difficulty_query = {"recDifficulty": difficulty_query_op}
        else:
            difficulty_query = {}

        question_ids = QuizzrTPM.get_ids(qtpm.unrec_questions, difficulty_query)
        if not question_ids:
            logging.error(f"No unrecorded questions found for difficulty type '{difficulty}'. Aborting")
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

    # Submit a recording for pre-screening and upload it to the database if it passes.
    @app.route("/upload", methods=["POST"])
    def pre_screen():
        valid_rec_types = ["normal", "buzz"]
        recording = request.files.get("audio")
        question_id = request.form.get("qid")
        diarization_metadata = request.form.get("diarMetadata")
        rec_type = request.form.get("recType")

        logging.debug(f"question_id = {question_id!r}")
        logging.debug(f"diarization_metadata = {diarization_metadata!r}")
        logging.debug(f"rec_type = {rec_type!r}")

        if recording is None:
            logging.error("No audio recording defined. Aborting")
            return "arg_audio_undefined", HTTPStatus.BAD_REQUEST

        if rec_type is None:
            logging.error("Form argument 'recType' is undefined. Aborting")
            return "arg_recType_undefined", HTTPStatus.BAD_REQUEST
        elif rec_type not in valid_rec_types:
            logging.error(f"Invalid rec type {rec_type!r}. Aborting")
            return "arg_recType_invalid", HTTPStatus.BAD_REQUEST
        qid_required = rec_type != "buzz"
        qid_used = qid_required or rec_type != "buzz"  # Redundant but for flexibility

        question_id, success = error_handling.to_oid_soft(question_id)
        if qid_used:
            if qid_required and question_id is None:
                logging.error("Form argument 'qid' expected. Aborting")
                return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
            if not success:
                logging.error("Form argument 'qid' is not a valid ObjectId. Aborting")
                return "arg_qid_invalid", HTTPStatus.BAD_REQUEST

        user_ids = QuizzrTPM.get_ids(qtpm.users)
        if not user_ids:
            logging.error("No user IDs found. Aborting")
            return "empty_uids", HTTPStatus.INTERNAL_SERVER_ERROR
        # user_ids = qtpm.user_ids.copy()
        while True:
            user_id, success = error_handling.to_oid_soft(random.choice(user_ids))
            if success:
                break
            logging.warning(f"Found malformed user ID {user_id}. Retrying...")
            user_ids.remove(user_id)
            if not user_ids:
                logging.warning("Could not find properly formed user IDs. Proceeding with last choice")
                break
        logging.debug(f"user_id = {user_id!r}")

        metadata = {
            "recType": rec_type,
            "userId": user_id
        }
        if diarization_metadata:
            metadata["diarMetadata"] = diarization_metadata
        if question_id:
            metadata["questionId"] = question_id

        submission_name = _save_recording(rec_dir, recording, metadata)
        logging.debug(f"submission_name = {submission_name!r}")
        try:
            # accepted_submissions, errors = qp.pick_submissions(
            #     rec_processing.QuizzrWatcher.queue_submissions(qtpm.REC_DIR)
            # )
            results = qp.pick_submissions(
                rec_processing.QuizzrWatcher.queue_submissions(qtpm.REC_DIR, size_limit=app.config["REC_QUEUE_LIMIT"])
            )
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
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
            file2gfid = qtpm.gdrive_upload_many(normal_file_paths, "/")
            file2gfid.update(qtpm.gdrive_upload_many(buzz_file_paths, "/Buzz"))
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR

        # sub2gfid = {os.path.splitext(file)[0]: file2gfid[file] for file in file2gfid}
        sub2meta = {}
        sub2vtt = {}

        for submission in results:
            doc = results[submission]
            sub2meta[submission] = doc["metadata"]
            if "vtt" in doc:
                sub2vtt[submission] = doc.get("vtt")

        # Insert submissions
        qtpm.mongodb_insert_submissions(
            sub2gfid={os.path.splitext(file)[0]: file2gfid[file] for file in file2gfid},
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

    # Upload a batch of unrecorded questions.
    @app.route("/upload/question", methods=["POST"])
    def upload_questions():
        arguments_batch = request.get_json()
        arguments_list = arguments_batch["arguments"]
        logging.debug(f"arguments_list = {arguments_list!r}")

        logging.info(f"Uploading {len(arguments_list)} unrecorded questions...")
        qtpm.insert_unrec_questions(arguments_list)
        logging.info("Successfully uploaded questions")
        return {"msg": "unrec_question.upload_success"}

    # DO NOT INCLUDE THE ROUTES BELOW IN DEPLOYMENT
    @app.route("/uploadtest/")
    def recording_listener_test():
        if app.config["Q_ENV"] not in ["development", "testing"]:
            abort(HTTPStatus.NOT_FOUND)
        return render_template("uploadtest.html")

    # Return the next submission name.
    def _get_next_submission_name():
        return str(datetime.now().strftime("%Y.%m.%d %H.%M.%S.%f"))

    # Write a WAV file and its JSON metadata to disk.
    def _save_recording(directory, recording, metadata):
        logging.info("Saving recording...")
        submission_name = _get_next_submission_name()
        logging.debug(f"submission_name = {submission_name!r}")
        submission_path = os.path.join(directory, submission_name)
        recording.save(submission_path + ".wav")
        logging.info("Saved recording successfully")

        logging.info("Writing metadata...")
        logging.debug(f"metadata = {metadata!r}")
        with open(submission_path + ".json", "w") as transaction_f:
            transaction_f.write(bson.json_util.dumps(metadata))
        logging.info("Successfully wrote metadata")
        return submission_name

    return app
