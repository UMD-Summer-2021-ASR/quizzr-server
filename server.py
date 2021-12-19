import argparse
import json
import logging
import logging.handlers
import multiprocessing
import os
import pprint
import queue
import random
import re
import string
import time
from copy import deepcopy
from sys import exit
from datetime import datetime, timedelta
from http import HTTPStatus
from secrets import token_urlsafe
from typing import List, Union, Tuple, Dict, Any, Optional

import jsonschema.exceptions
import pymongo.errors
import werkzeug.datastructures
from firebase_admin import auth
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
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

logging.basicConfig(level=os.environ.get("QUIZZR_LOG") or "INFO")

DEV_ENV_NAME = "development"
PROD_ENV_NAME = "production"
TEST_ENV_NAME = "testing"


# TODO: Re-implement QuizzrWatcher through the Celery framework for Flask.
def create_app(test_overrides: dict = None, test_inst_path: str = None, test_storage_root: str = None):
    """
    App factory function for the data flow server

    :param test_overrides: A set of overrides to merge on top of the server's configuration
    :param test_inst_path: The instance path of the server. Has the highest overriding priority
    :param test_storage_root: The root directory to use in place of the "storage" directory
    :return: The data flow server as a Flask app
    """

    app_attributes = {}

    instance_path = test_inst_path or os.environ.get("Q_INST_PATH")\
        or os.path.expanduser(os.path.join("~", "quizzr_server"))
    app = Flask(
        __name__,
        instance_relative_config=True,
        instance_path=instance_path
    )
    storage_root = test_storage_root or os.environ.get("Q_STG_ROOT") or os.path.join(app.instance_path, "storage")
    log_dir = os.path.join(storage_root, "logs")
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    log_path = os.path.join(log_dir, "sv_log.log")
    handler = logging.handlers.TimedRotatingFileHandler(log_path, when='h', interval=12, backupCount=13)
    formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(name)s: %(message)s")
    handler.setFormatter(formatter)
    app.logger.addHandler(handler)
    app.logger.info(f"Instantiated server with instance path '{instance_path}'")
    CORS(app)
    app.logger.info("Creating instance directory...")
    server_dir = os.path.dirname(__file__)
    os.makedirs(app.instance_path, exist_ok=True)
    app.logger.info("Created instance directory")
    app.logger.info("Configuring server instance...")
    default_config = {
        "UNPROC_FIND_LIMIT": 32,
        "DATABASE": "QuizzrDatabase",
        "BLOB_ROOT": "production",
        # "BLOB_NAME_LENGTH": 32,
        "Q_ENV": PROD_ENV_NAME,
        "SUBMISSION_FILE_TYPES": ["wav", "json", "vtt"],
        # "DIFFICULTY_LIMITS": [3, 6, None],
        "DIFFICULTY_DIST": [0.6, 0.3, 0.1],
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
                "projection": None,
                "collection": "Users"
            }
        },
        "USE_ID_TOKENS": True,
        "MAX_LEADERBOARD_SIZE": 200,
        "DEFAULT_LEADERBOARD_SIZE": 10,
        "MAX_USERNAME_LENGTH": 16,
        "USERNAME_CHAR_SET": string.ascii_letters + string.digits,
        "DEFAULT_RATE_LIMITS": []
    }

    config_dir = os.path.join(app.instance_path, "config")
    if not os.path.exists(config_dir):
        os.mkdir(config_dir)
    conf_name = os.environ.get("Q_CONFIG_NAME") or "sv_config.json"
    app.logger.info(f"Using config with name '{conf_name}'")
    conf_path = os.path.join(config_dir, conf_name)

    app_conf = default_config
    if os.path.exists(conf_path):
        with open(conf_path, "r") as config_f:
            config = json.load(config_f)
        app_conf.update(config)
    else:
        app.logger.info(f"Config at path '{conf_path}' not found")

    # env_cfg = {}
    #
    # for key in app_conf:
    #     # if type(app_conf[var]) is not str and var in os.environ:
    #     #     app.logger.critical(f"Cannot override non-string config field '{var}' through environment variable")
    #     #     exit(1)
    #     env_key = "DF_CFG_" + key
    #     if env_key in os.environ:
    #         env_cfg_val = os.environ.get(env_key)
    #         if env_cfg_val:
    #             try:
    #                 env_cfg[key] = json.loads(env_cfg_val)
    #             except json.decoder.JSONDecodeError:
    #                 env_cfg[key] = env_cfg_val
    # app_conf.update(env_cfg)
    if test_overrides:
        app_conf.update(test_overrides)

    rec_dir = os.path.join(storage_root, "recordings")
    if not os.path.exists(rec_dir):
        os.makedirs(rec_dir)

    queue_dir = os.path.join(rec_dir, "queue")
    error_dir = os.path.join(rec_dir, "_error")

    secret_dir = os.path.join(app.instance_path, "secrets")
    if not os.path.exists(secret_dir):
        os.makedirs(secret_dir)

    app_conf["REC_DIR"] = rec_dir
    api = QuizzrAPISpec(os.path.join(server_dir, "reference", "backend.yaml"))

    app.config.from_mapping(app_conf)

    # Validate configuration values
    if app.config["DEFAULT_LEADERBOARD_SIZE"] > app.config["MAX_LEADERBOARD_SIZE"]:
        app.logger.critical("Configured default leaderboard size must not exceed maximum leaderboard size.")
        exit(1)

    for percentage in app.config["DIFFICULTY_DIST"]:
        if not (0 <= percentage <= 1):
            app.logger.critical("Configured DIFFICULTY_DIST percentages must be between 0 and 1.")
            exit(1)

    app.logger.info("Finished configuring server instance")

    app.logger.info("Initializing third-party services...")
    app.logger.info(f"MongoDB Database Name = '{app_conf['DATABASE']}'")
    app.logger.info(f"Firebase Blob Root = '{app_conf['BLOB_ROOT']}'")
    app.logger.info(f"Environment set to '{app_conf['Q_ENV']}'")
    qtpm = QuizzrTPM(app_conf["DATABASE"], app_conf, os.path.join(secret_dir, "firebase_storage_key.json"),
                     app.logger.getChild("qtpm"))
    app.logger.info("Initialized third-party services")

    # app.logger.info("Initializing pre-screening program...")
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
    prescreen_results_queue = multiprocessing.Queue()
    qw_process = multiprocessing.Process(target=rec_processing.start_watcher, kwargs={
        "db_name": app_conf["DATABASE"],
        "tpm_config": app_conf,
        "firebase_app_specifier": qtpm.app,
        "rec_dir": rec_dir,
        "queue_dir": queue_dir,
        "error_dir": error_dir,
        "proc_config": app_conf["PROC_CONFIG"],
        "submission_file_types": app_conf["SUBMISSION_FILE_TYPES"],
        "queue": prescreen_results_queue,
        "logger": app.logger.getChild("prescreen")
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

    # def restart_qw_process(signal_, frame):
    #     app.logger.info("Received SIGCHLD")
    #     app.logger.debug(f"app_attributes = {app_attributes}")
    #     pid, stat = os.waitpid(0, 0)
    #     if "qwPid" not in app_attributes or pid != app_attributes["qwPid"]:
    #         app.logger.info("pid does not match or qwPid not set. Ignoring")
    #         return
    #     app.logger.debug(f"stat = {stat}")
    #     app.logger.info("Restarting pre-screening program...")
    #     if "qwStartTime" in app_attributes:
    #         since_prev_restart = time.time() - app_attributes["qwStartTime"]
    #         if since_prev_restart < app.config["QW_SHUTDOWN_INTERVAL_THRESHOLD"]:
    #             # Might be better to make it use a backoff strategy.
    #             app.logger.critical("Unable to start up pre-screening program. Forcing shutdown...")
    #             exit(1)
    #     app_attributes["qwStartTime"] = time.time()
    #     qw_process.start()
    #     app_attributes["qwPid"] = qw_process.pid
    #     app.logger.debug(f"qwPid = {app_attributes['qwPid']}")

    # if not app.config["DEBUG"]:
    #     old_sigh = signal.signal(signal.SIGCHLD, restart_qw_process)
    #     app.logger.debug(f"old_sigh = {old_sigh}")
    app_attributes["qwStartTime"] = time.time()
    qw_process.start()
    app_attributes["qwPid"] = qw_process.pid
    app.logger.debug(f"qwPid = {app_attributes['qwPid']}")
    app.logger.info("Started pre-screening program")

    rate_limits = app.config["DEFAULT_RATE_LIMITS"]
    if rate_limits:
        app.logger.info("Initializing rate limiter...")
        limiter = Limiter(app, key_func=get_remote_address, default_limits=rate_limits)
        app.logger.info("Finished initializing rate limiter")
    else:
        app.logger.info("No default rate limits defined. Skipping rate limiter initialization")

    secret_keys = {}
    prescreen_statuses = []
    pprinter = pprint.PrettyPrinter()

    app.logger.info("Completed initialization")

    @app.route("/audio", methods=["GET", "POST", "PATCH"])
    def audio_resource():
        """
        GET: Get a batch of at most ``UNPROC_FIND_LIMIT`` documents from the UnprocessedAudio collection in the MongoDB
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
            diarization_metadata_list = request.form.getlist("diarMetadata")
            rec_types = request.form.getlist("recType")
            expected_answers = request.form.getlist("expectedAnswer")
            transcripts = request.form.getlist("transcript")
            correct_flags = request.form.getlist("correct")

            _debug_variable("recordings", recordings)
            _debug_variable("qb_ids", qb_ids)
            _debug_variable("sentence_ids", sentence_ids)
            _debug_variable("diarization_metadata_list", diarization_metadata_list)
            _debug_variable("rec_types", rec_types)
            _debug_variable("expected_answers", expected_answers)
            _debug_variable("transcripts", transcripts)
            _debug_variable("correct_flags", correct_flags)

            if not (len(recordings) == len(rec_types)
                    and (not qb_ids or len(recordings) == len(qb_ids))
                    and (not sentence_ids or len(recordings) == len(sentence_ids))
                    and (not diarization_metadata_list or len(recordings) == len(diarization_metadata_list))
                    and (not expected_answers or len(recordings) == len(expected_answers))
                    and (not transcripts or len(recordings) == len(transcripts))
                    and (not correct_flags or len(recordings) == len(correct_flags))):
                return _make_err_response(
                    "Received incomplete form batch",
                    "incomplete_batch",
                    HTTPStatus.BAD_REQUEST,
                    log_msg=True
                )

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
            return pre_screen(recordings, rec_types, user_id, qb_ids, sentence_ids, diarization_metadata_list,
                              expected_answers, transcripts, correct_flags)
        elif request.method == "PATCH":
            arguments_batch = request.get_json()
            return handle_processing_results(arguments_batch)

    def get_unprocessed_audio():
        """
        Get a batch of at most ``UNPROC_FIND_LIMIT`` documents from the UnprocessedAudio collection in the MongoDB
        Atlas.

        :return: A dictionary containing an array of dictionaries under the key "results"
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
            if "tokenizationId" in audio_doc:
                entry["tokenizationId"] = audio_doc["tokenizationId"]
            id2entries[id_key].append(entry)
            qids.append(qid)
            audio_doc_count += 1

        if audio_doc_count == 0:
            app.logger.error("Could not find any audio documents")
            return _make_err_response(
                "Could not find any audio documents",
                "empty_unproc_audio",
                HTTPStatus.NOT_FOUND
            )

        _debug_variable("id2entries", id2entries)
        app.logger.info(f"Found {audio_doc_count} unprocessed audio document(s)")
        if not qids:
            app.logger.error("No audio documents contain question IDs")
            return _make_err_response(
                "No audio documents contain question IDs",
                "empty_qid2entries",
                HTTPStatus.NOT_FOUND
            )

        question_gen = qtpm.find_questions(qids)
        for question in question_gen:
            _debug_variable("question", question)
            qid = question["qb_id"]
            sentence_id = question.get("sentenceId")
            id_key = "_".join([str(qid), str(sentence_id)]) if sentence_id else str(qid)
            orig_transcript = question.get("transcript")
            if orig_transcript:
                entries = id2entries[id_key]
                for entry in entries:
                    if "tokenizations" in question and "tokenizationId" in entry:
                        slice_start, slice_end = question["tokenizations"][entry.pop("tokenizationId")]
                        transcript = orig_transcript[slice_start:slice_end]
                    else:
                        transcript = orig_transcript
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
                   diarization_metadata_list: List[str] = None,
                   expected_answers: List[str] = None,
                   transcripts: List[str] = None,
                   correct_flags: List[str] = None) -> Tuple[Union[dict, str], int]:
        """
        Submit one or more recordings for pre-screening and uploading.

        WARNING: Submitting recordings of different ``rec_types`` in the same batch can give unpredictable results.

        :param recordings: A list of audio files
        :param rec_types: A list of rec_types associated with each recording
        :param user_id: The ID of the submitter
        :param qb_ids: (optional) A list of question IDs associated with each recording
        :param sentence_ids: (optional) The associated sentence IDs. Only required for segmented questions
        :param diarization_metadata_list: (optional) The associated parameter sets for diarization
        :param expected_answers: (optional) A list of the associated expected answers (for "answer" recordings)
        :param transcripts: (optional) A list of the associated output transcripts (for "answer" recordings)
        :param correct_flags: (optional) Whether each "answer" recording contains the correct answer
        :return: A dictionary with the key "prescreenPointers" and a status code. If an error occurred, a string with a
                 status code is returned instead.
        """
        valid_rec_types = ["normal", "buzz", "answer"]

        pointers = []
        submissions = []

        for i in range(len(recordings)):
            recording = recordings[i]
            rec_type = rec_types[i]
            qb_id = qb_ids[i] if len(qb_ids) > i else None
            sentence_id = sentence_ids[i] if len(sentence_ids) > i else None
            diarization_metadata = diarization_metadata_list[i] if len(diarization_metadata_list) > i else None
            expected_answer = expected_answers[i] if len(expected_answers) > i else None
            transcript = transcripts[i] if len(transcripts) > i else None
            correct = correct_flags[i] if len(correct_flags) > i else None

            _debug_variable("qb_id", qb_id)
            _debug_variable("sentence_id", sentence_id)
            _debug_variable("diarization_metadata", diarization_metadata)
            _debug_variable("rec_type", rec_type)
            _debug_variable("expected_answer", expected_answer)
            _debug_variable("transcript", transcript)
            _debug_variable("correct", correct)

            if not recording:
                return _make_err_response(
                    "Argument 'audio' is undefined",
                    "undefined_arg",
                    HTTPStatus.BAD_REQUEST,
                    ["audio"],
                    True
                )

            if not rec_type:
                return _make_err_response(
                    "Argument 'recType' is undefined",
                    "undefined_arg",
                    HTTPStatus.BAD_REQUEST,
                    ["recType"],
                    True
                )
            elif rec_type not in valid_rec_types:
                return _make_err_response(
                    f"Invalid rec type: '{rec_type!r}'",
                    "invalid_arg",
                    HTTPStatus.BAD_REQUEST,
                    ["invalid_recType"],
                    True
                )

            qid_required = rec_type not in ["buzz", "answer"]
            if qid_required and not qb_id:
                return _make_err_response(
                    "Form argument 'qb_id' expected",
                    "undefined_arg",
                    HTTPStatus.BAD_REQUEST,
                    ["qb_id"],
                    True
                )

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
                metadata["tokenizationId"] = i
            if expected_answer:
                metadata["expectedAnswer"] = expected_answer
            if transcript:
                metadata["transcript"] = transcript
            if correct:
                metadata["correct"] = correct.lower() == "true"

            submissions.append((recording, metadata))

        if len(submissions) == 1:
            submission_names = [_save_recording(queue_dir, *submissions[0])]
        else:
            submission_names = _save_recording_batch(queue_dir, submissions)

        for submission_name in submission_names:
            _debug_variable("submission_name", submission_name)
            pointer = token_urlsafe(64)
            expiry = datetime.now() + timedelta(minutes=30)
            ps_doc = {"pointer": pointer, "name": submission_name, "status": "running", "expiry": expiry}
            prescreen_statuses.append(ps_doc)
            pointers.append(pointer)

        return {"prescreenPointers": pointers}, HTTPStatus.ACCEPTED

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
        """
        Attach the given arguments to multiple unprocessed audio documents and move them to the Audio collection.
        Additionally, update the recording history of the associated questions and users.

        :param arguments_batch: A list of documents each containing the fields to update along with the ID of the
                                document to update
        :return: The number of update documents received, the number of successful updates, and the errors that
                 occurred, all in one dictionary
        """
        arguments_list = arguments_batch.get("arguments")
        if arguments_list is None:
            # return "undefined_arguments", HTTPStatus.BAD_REQUEST
            return _make_err_response(
                "Argument 'arguments' is undefined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["arguments"],
                True
            )
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
        """Check if an answer is correct using approximate string matching."""
        qid = request.args.get("qid")
        user_answer = request.args.get("a")

        if qid is None or qid == '':
            # return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
            return _make_err_response(
                "Query parameter 'qid' is undefined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["qid"],
                True
            )
        if not user_answer:
            # return "arg_a_undefined", HTTPStatus.BAD_REQUEST
            return _make_err_response(
                "Query parameter 'a' is undefined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["a"],
                True
            )

        question = qtpm.rec_questions.find_one({"qb_id": int(qid)})
        if not question:
            # return "question_not_found", HTTPStatus.NOT_FOUND
            return _make_err_response(
                "Could not find question",
                "question_not_found",
                HTTPStatus.NOT_FOUND,
                log_msg=True
            )
        correct_answer = question.get("answer")
        if not correct_answer:
            # return "answer_not_found", HTTPStatus.NOT_FOUND
            return _make_err_response(
                "Question does not contain field 'answer'",
                "answer_not_found",
                HTTPStatus.NOT_FOUND,
                log_msg=True
            )

        answer_similarity = fuzz.token_set_ratio(user_answer, correct_answer)
        _debug_variable("answer_similarity", answer_similarity)
        return {"correct": answer_similarity >= app.config["MIN_ANSWER_SIMILARITY"]}

    @app.route("/answer_full/<int:qid>", methods=["GET"])
    def get_answer(qid):
        """Get the answer of a question. This is intended for use by a backend component."""
        # if qid is None:
        #     # return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
        #     return _make_err_response(
        #         "Path parameter is undefined",
        #         "undefined_arg",
        #         HTTPStatus.BAD_REQUEST,
        #         ["path"],
        #         True
        #     )

        question = qtpm.rec_questions.find_one({"qb_id": qid})
        if not question:
            # return "question_not_found", HTTPStatus.NOT_FOUND
            return _make_err_response(
                "Could not find question",
                "question_not_found",
                HTTPStatus.NOT_FOUND,
                log_msg=True
            )
        correct_answer = question.get("answer")
        if not correct_answer:
            # return "answer_not_found", HTTPStatus.NOT_FOUND
            return _make_err_response(
                "Question does not contain field 'answer'",
                "answer_not_found",
                HTTPStatus.NOT_FOUND,
                log_msg=True
            )

        return {"answer": correct_answer}

    @app.route("/audio/<path:blob_path>", methods=["GET"])
    def retrieve_audio_file(blob_path):
        """
        Retrieve a file from Firebase Storage. The blob path is usually in the format ``<recType>/<_id>``.

        :param blob_path: The path to a Firebase Cloud Storage object
        :return: A response containing the bytes of the audio file
        """
        return send_file(qtpm.get_file_blob(blob_path), mimetype="audio/wav")

    @app.post("/socket/key")
    def generate_game_key():
        """
        Alias for ``/backend/key`` with the "name" argument being "socket".

        **WARNING: A secret key can be generated only once per server session. This key cannot be recovered if lost.**
        """
        return _gen_secret_key("socket")

    @app.route("/downvote/<audio_id>", methods=["PATCH"])
    def downvote(audio_id):
        """Downvote an audio recording."""
        args = request.get_json()
        uid = args["userId"]
        if "userId" not in args:
            return _make_err_response(
                "Argument 'userId' is undefined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                log_msg=True
            )

        user = qtpm.users.find_one({"_id": uid})
        rec_votes = user.get("recVotes") or []
        has_voted = False
        for i, rec_vote in enumerate(rec_votes):
            if rec_vote["id"] == audio_id:
                has_voted = True
                if rec_vote["vote"] == -1:  # Avoid downvoting twice.
                    return '', HTTPStatus.OK
                else:
                    qtpm.users.update_one({"_id": uid}, {"$set": {f"recVotes.{i}.vote": -1}})
                    qtpm.audio.update_one({"_id": audio_id}, {"$inc": {"upvotes": -1}})
                    break

        if not has_voted:
            qtpm.users.update_one({"_id": uid}, {"$push": {"recVotes": {"id": audio_id, "vote": -1}}})

        result = qtpm.audio.update_one({"_id": audio_id}, {"$inc": {"downvotes": 1}})

        if result.matched_count == 0:
            return _make_err_response(
                f"Audio document with ID '{audio_id}' not found",
                "doc_not_found",
                HTTPStatus.NOT_FOUND,
                [audio_id],
                True
            )
        return '', HTTPStatus.OK

    @app.put("/game_results")
    def handle_game_results():
        """
        Update the database with the results of a game session.

        Precondition: ``session_results["questionStats"]["played"] != 0``
        """

        session_results = request.get_json()
        _debug_variable("session_results", session_results)
        if "category" in session_results:
            return handle_game_results_category(session_results)
        if "categories" in session_results:
            return handle_game_results_categories(session_results)
        return _make_err_response(
            "Arguments 'category' or 'categories' not provided",
            "undefined_args",
            HTTPStatus.BAD_REQUEST,
            ["category_or_categories"],
            True
        )

    # @app.route("/game", methods=["GET", "POST"])
    # def game_resource():
    #     """Resource for sharing game sessions"""
    #     if request.method == "GET":
    #         return get_game()
    #     elif request.method == "POST":
    #         return post_game()

    @app.get("/game/<game_id>")
    def get_game(game_id):
        """Get a game session from MongoDB."""
        if not game_id:
            return _make_err_response(
                "Path parameter not defined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["path"],
                True
            )

        session = qtpm.games.find_one({"_id": game_id})
        if session is None:
            return _make_err_response(
                f"Game session with ID '{game_id}' not found",
                "resource_not_found",
                HTTPStatus.NOT_FOUND,
                [game_id],
                True
            )

        return session

    @app.post("/game")
    def post_game():
        """Upload a game session to MongoDB for users to share."""
        arguments = request.get_json()

        if arguments is None:
            return _make_err_response(
                "No arguments specified",
                "no_args",
                HTTPStatus.BAD_REQUEST,
                log_msg=True
            )

        game_id = arguments.get("id")
        session = arguments.get("session")

        if not game_id:
            return _make_err_response(
                "Argument 'id' not defined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["id"],
                True
            )

        if not session:
            return _make_err_response(
                "Argument 'session' not defined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["session"],
                True
            )

        if "settings" not in session:
            return _make_err_response(
                "Session does not contain a 'settings' field",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["settings"],
                True
            )

        if "players" not in session["settings"]:
            return _make_err_response(
                "Session settings do not contain a list of players",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["players"],
                True
            )

        session["_id"] = game_id

        try:
            qtpm.games.insert_one(session)
        except pymongo.errors.DuplicateKeyError:
            return _make_err_response(
                f"ID for game session {game_id} already occupied",
                "id_taken",
                HTTPStatus.BAD_REQUEST,
                [game_id],
                True
            )

        update_game_histories(session)

        return '', HTTPStatus.CREATED

    def update_game_histories(session):
        """
        Update the "history" field of every player in the "settings" of the given session

        :param session: The metadata of a game session. Requires a "settings" field that contains the "players" field,
                        an array of user IDs.
        """
        update_batch = []

        for player in session["settings"]["players"]:
            update_batch.append(UpdateOne({"_id": player}, {"$push": {"history": session}}))
        
        if len(update_batch) == 0:
            return

        qtpm.users.bulk_write(update_batch)

    def handle_game_results_category(session_results):
        """
        Update the database with the results of a game session with only one category.

        :param session_results: A dictionary containing the "mode", "category", and the update arguments for each user,
                                under the key "users"
        :return: A dictionary containing the number of "successful" and "requested" updates
        """
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

        if update_batch:
            app.logger.info("Sending bulk write operation...")
            results = qtpm.users.bulk_write(update_batch)
            app.logger.info(f"Matched {results.matched_count} documents and modified {results.modified_count} documents")
            app.logger.info(f"Request body contained profile updates for {len(session_results['users'])} users")
            return {"successful": results.matched_count, "requested": len(session_results["users"])}
        else:
            app.logger.info("Bulk write operation is empty. Skipping")
            return {"successful": 0, "requested": 0}

    def handle_game_results_categories(session_results):
        """
        Update the database with the results of a game session with multiple categories.

        :param session_results: A dictionary containing the "mode", "categories", and the update arguments for each
                                user, under the key "users"
        :return: A dictionary containing the number of "successful" and "requested" updates
        """
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

        if update_batch:
            app.logger.info("Sending bulk write operation...")
            results = qtpm.users.bulk_write(update_batch)
            app.logger.info(f"Matched {results.matched_count} documents and modified {results.modified_count} documents")
            app.logger.info(f"Request body contained profile updates for {len(session_results['users'])} users")
            return {"successful": results.matched_count, "requested": len(session_results["users"])}
        else:
            app.logger.info("Bulk write operation is empty. Skipping")
            return {"successful": 0, "requested": 0}

    @app.post("/backend/key")
    def generate_secret_key():
        """
        Generate a secret key to represent a given backend component.

        **WARNING: A secret key can be generated only once per server session. This key cannot be recovered if lost.**
        """

        args = request.get_json()
        if args is None:
            return _make_err_response(
                "No arguments specified",
                "no_args",
                HTTPStatus.BAD_REQUEST,
                log_msg=True
            )
        if "name" not in args:
            return _make_err_response(
                "Argument 'name' not provided",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                ["name"],
                True
            )
        component_name = args["name"]
        return _gen_secret_key(component_name)

    @app.route("/hls/vtt/<audio_id>", methods=["GET"])
    def get_vtt(audio_id):
        """
        Download the VTT of an audio document.

        :param audio_id: The ID of the document in the Audio collection
        :return: A response containing the VTT as an octet stream
        """

        type_arg = request.args.get("t") or "normal"
        if type_arg == "gentle":
            field_name = "gentleVtt"
        elif type_arg == "normal":
            field_name = "vtt"
        else:
            app.logger.error(f"No such VTT type '{type_arg}'. Aborting")
            return _make_err_response(
                f"No such VTT type '{type_arg}'. Valid types: {', '.join(['gentle', 'normal'])}",
                "invalid_arg",
                HTTPStatus.INTERNAL_SERVER_ERROR,
                ["t", type_arg]
            )
        audio_doc = qtpm.audio.find_one({"_id": audio_id}, {field_name: 1})
        if audio_doc is None or audio_doc.get(field_name) is None:
            abort(HTTPStatus.NOT_FOUND)
        response = make_response(bytes(audio_doc[field_name], "utf-8"))
        response.headers["Content-Type"] = "application/octet-stream"
        return response

    @app.route("/leaderboard", methods=["GET"])
    def get_leaderboard():
        """
        Get the basic profiles of the top ``size`` players.

        :return: A dictionary containing the "results"
        """
        category = request.args.get("category") or "all"
        arg_size = request.args.get("size")
        size = arg_size or app.config["DEFAULT_LEADERBOARD_SIZE"]
        if size > app.config["MAX_LEADERBOARD_SIZE"]:
            return _make_err_response(
                f"Given size exceeds allowable limit ({app.config['MAX_LEADERBOARD_SIZE']})",
                "invalid_arg",
                HTTPStatus.BAD_REQUEST,
                ["exceeds_value", app.config["MAX_LEADERBOARD_SIZE"]],
                True
            )
        visibility_config = app.config["VISIBILITY_CONFIGS"]["basic"]
        cursor = qtpm.database.get_collection(visibility_config["collection"]).find(
            {f"ratings.{category}": {"$exists": True}},
            sort=[(f"ratings.{category}", pymongo.DESCENDING)],
            limit=size,
            projection=visibility_config["projection"]
        )
        return {"results": [doc for doc in cursor]}

    @app.route("/prescreen/<pointer>", methods=["GET"])
    def get_prescreen_status(pointer):
        """
        Get the status of a submission using a short-lived pointer.

        :param pointer: A pointer retrieved from pre_screen
        :return: A document containing the name, status, and other information of a submission
        """
        status_doc = _get_ps_doc(pointer=pointer)
        if status_doc is None:
            app.logger.error(f"No such resource for pointer '{pointer}'. Aborting")
            return _make_err_response(
                "No such resource",
                "resource_not_found",
                HTTPStatus.NOT_FOUND
            )
        if datetime.now() > status_doc["expiry"]:
            app.logger.error(f"Resource for pointer '{pointer}' has expired. Aborting")
            return _make_err_response(
                "The resource to access the status of this submission has expired.",
                "expired_resource",
                HTTPStatus.NOT_FOUND
            )
        _update_prescreen_statuses()
        result = status_doc.copy()
        del result["expiry"]
        return result

    @app.route("/profile", methods=["GET", "POST", "PATCH", "DELETE"])
    def own_profile():
        """
        A resource to automatically point to the user's own profile

        GET: Retrieve the profile

        POST: Create a new profile

        PATCH: Modify the profile

        DELETE: Remove the profile
        """
        decoded = _verify_id_token()
        user_id = decoded["uid"]
        if request.method == "GET":
            result = qtpm.get_profile(user_id, "private")
            if result is None:
                app.logger.error(f"User with ID '{user_id}' does not have a profile. Aborting")
                abort(HTTPStatus.NOT_FOUND)
            return result
        elif request.method == "POST":
            args = request.get_json()
            _debug_variable("args", args)
            if len(args["username"]) == 0:
                return _make_err_response(
                    "Username is empty",
                    "invalid_args",
                    HTTPStatus.BAD_REQUEST,
                    ["username", "empty"],
                    True
                )
            if len(args["username"]) > app.config["MAX_USERNAME_LENGTH"]:
                return _make_err_response(
                    f"Username exceeds maximum length: {app.config['MAX_USERNAME_LENGTH']}",
                    "invalid_args",
                    HTTPStatus.BAD_REQUEST,
                    ["username", "exceeds_max_length"],
                    True
                )

            char_set = app.config["USERNAME_CHAR_SET"]
            for char in args["username"]:
                if char not in char_set:
                    return _make_err_response(
                        f"Username contains characters outside char set: {char_set}",
                        "invalid_args",
                        HTTPStatus.BAD_REQUEST,
                        ["username", "outside_char_set", "alphanumeric"],
                        True
                    )

            try:
                result = qtpm.create_profile(user_id, args["pfp"], args["username"])
            except pymongo.errors.DuplicateKeyError:
                return _make_err_response(
                    "User already registered",
                    "already_registered",
                    HTTPStatus.BAD_REQUEST,
                    log_msg=True
                )
            except UsernameTakenError as e:
                return _make_err_response(
                    f"Username already exists: {e}",
                    "username_exists",
                    HTTPStatus.BAD_REQUEST,
                    [str(e)],
                    True
                )
            if result:
                app.logger.info("User successfully created")
                # return {"msg": "user_created"}, HTTPStatus.CREATED
                return '', HTTPStatus.CREATED
            app.logger.error("Encountered an unknown error")
            return _make_err_response(
                "Encountered an unknown error",
                "unknown_error",
                HTTPStatus.INTERNAL_SERVER_ERROR
            )
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
                # app.logger.error(f"Username already taken: {e}. Aborting")
                # return f"username_taken: {e}", HTTPStatus.BAD_REQUEST
                return _make_err_response(
                    f"Username already exists: {e}",
                    "username_exists",
                    HTTPStatus.BAD_REQUEST,
                    [str(e)],
                    True
                )

            if result.matched_count == 1:
                app.logger.info("User profile successfully modified")
                # return {"msg": "user_modified"}, HTTPStatus.OK
                return '', HTTPStatus.OK
            return _make_err_response(
                f"No such user: {user_id}",
                "user_not_found",
                HTTPStatus.NOT_FOUND,
                [user_id],
                True
            )
        elif request.method == "DELETE":
            result = qtpm.delete_profile(user_id)
            if result.deleted_count == 1:
                app.logger.info("User profile successfully deleted")
                # return {"msg": "user_deleted"}, HTTPStatus.OK
                return '', HTTPStatus.OK
            return _make_err_response(
                f"No such user: {user_id}",
                "user_not_found",
                HTTPStatus.NOT_FOUND,
                [user_id],
                True
            )

    @app.route("/profile/<username>", methods=["GET", "PATCH", "DELETE"])
    def other_profile(username):
        """
        A resource for the profile of any user

        GET: Get the profile

        PATCH: Modify the profile

        DELETE: Remove the profile
        """
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
            if result.matched_count == 1:
                app.logger.info("User profile successfully modified")
                # return "other_user_modified", HTTPStatus.OK
                return '', HTTPStatus.OK
            return _make_err_response(
                f"No such user: {other_user_id}",
                "user_not_found",
                HTTPStatus.NOT_FOUND,
                [other_user_id],
                True
            )
        elif request.method == "DELETE":
            result = qtpm.delete_profile(other_user_id)
            if result.deleted_count == 1:
                app.logger.info("User profile successfully deleted")
                # return "other_user_deleted", HTTPStatus.OK
                return '', HTTPStatus.OK
            return _make_err_response(
                f"No such user: {other_user_id}",
                "user_not_found",
                HTTPStatus.NOT_FOUND,
                [other_user_id],
                True
            )

    @app.route("/profile/<username>/history", methods=["GET"])
    def get_game_history(username):
        """Retrieve the game history of a given username, optionally only including games from "start" to "end"."""
        index_range_arg = request.args.get("iRange")

        if index_range_arg:
            index_range = [int(num) for num in re.split(r",\s*", index_range_arg)]

            if len(index_range) != 2:
                return _make_err_response(
                    "Too many or too few items were provided for query parameter 'iRange'",
                    "invalid_length",
                    HTTPStatus.BAD_REQUEST,
                    [len(index_range)],
                    True
                )

            start = index_range[0]
            end = index_range[1] + 1
        else:
            start = request.args.get("start")
            end = request.args.get("end")

            if start is not None and start != "":
                start = int(start)
            else:
                start = None

            if end is not None and end != "":
                end = int(end) + 1
            else:
                end = None

        user_profile = qtpm.users.find_one({"username": username}, {"history": 1})

        if not user_profile:
            return _make_err_response(
                f"No such user with username '{username}'",
                "user_not_found",
                HTTPStatus.NOT_FOUND,
                [username],
                True
            )

        if "history" not in user_profile:
            return _make_err_response(
                f"Game history not found for user with username '{username}'",
                "history_not_found",
                HTTPStatus.NOT_FOUND,
                [username],
                True
            )

        history = user_profile["history"]
        try:
            history.sort(reverse=True, key=lambda session: datetime.strptime(session["date"], "%Y %m %d %H %M %S"))
        except KeyError as e:
            app.logger.exception("Encountered a KeyError while acquiring game history")
            return _make_err_response(
                f"Field {e} not found",  # No quotes because KeyError message already contains them
                "field_not_found",
                HTTPStatus.INTERNAL_SERVER_ERROR,
                [str(e)]
            )

        return {"results": history[start:end]}

    @app.route("/question", methods=["GET"])
    def pick_game_question():
        """Retrieve a batch of randomly-selected questions and attempt to retrieve the associated recordings with the
        best evaluations possible without getting recordings from different users in the same question."""
        categories = request.args.getlist("category")
        difficulty_range_arg = request.args.get("difficultyRange")
        batch_size = int(request.args.get("batchSize") or 1)
        pipeline = [
            {'$group': {'_id': '$qb_id', 'category': {'$first': '$category'}}},
            {'$match': {'$expr': {'$not': {'$eq': ['$_id', None]}}}},
            {'$sample': {'size': batch_size}},
            {'$lookup': {
                'from': 'Audio',
                'as': 'audio',
                'let': {'qb_id': '$_id'},
                'pipeline': [
                    {'$match': {'$expr': {'$eq': ['$qb_id', '$$qb_id']}, 'recType': 'normal'}}
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
            doc["audio"] = _pick_audio(doc["audio"])
            questions.append(doc)
        if not questions:
            return _make_err_response(
                "Could not find any questions",
                "questions_not_found",
                HTTPStatus.NOT_FOUND,
                log_msg=True
            )
        return {"results": questions}

    @app.route("/question/unrec", methods=["GET", "POST"])
    def unrec_question_resource():
        """
        Resource for unrecorded questions

        GET: Find a random unrecorded question (or multiple) and return the ID and transcript.

        POST: Upload a batch of unrecorded questions.
        """
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
        # if difficulty is not None:
        #     difficulty_query_op = QuizzrTPM.get_difficulty_query_op(app.config["DIFFICULTY_LIMITS"], difficulty)
        #     difficulty_query = {"recDifficulty": difficulty_query_op}
        # else:
        #     difficulty_query = {}
        #
        # query = {**difficulty_query, "qb_id": {"$exists": True}}
        #
        # cursor = qtpm.unrec_questions.find(query, {"qb_id": 1})

        query = {
            "qb_id": {"$exists": True},
            "recDifficulty": {"$exists": True}
        }

        if difficulty is not None:
            if difficulty > len(app.config["DIFFICULTY_DIST"]) - 1:
                return _make_err_response(
                    "Given difficulty type is too high",
                    "invalid_arg",
                    HTTPStatus.BAD_REQUEST,
                    log_msg=True
                )
            skip_percent = 0
            limit_percent = 0

            for i in range(0, difficulty):
                skip_percent += app.config["DIFFICULTY_DIST"][i]

            for i in range(0, difficulty + 1):
                limit_percent += app.config["DIFFICULTY_DIST"][i]

            doc_count = qtpm.unrec_questions.count_documents(query)
            skip = int(skip_percent * doc_count)
            limit = int(limit_percent * doc_count)

            _debug_variable("skip_percent", skip_percent)
            _debug_variable("limit_percent", limit_percent)
        else:
            skip, limit = 0, 0

        # Get IDs for unrecorded questions
        cursor = qtpm.unrec_questions.find(query, {"qb_id": 1}, skip=skip, limit=limit, sort=[("recDifficulty", pymongo.ASCENDING)])

        question_ids = list({doc["qb_id"] for doc in cursor})  # Ensure no duplicates are present
        if not question_ids:
            # app.logger.error(f"No unrecorded questions found for difficulty type '{difficulty}'. Aborting")
            # return "unrec_empty_qids", HTTPStatus.NOT_FOUND
            return _make_err_response(
                f"No unrecorded questions found for difficulty type '{difficulty}'",
                "unrec_empty_qids",
                HTTPStatus.NOT_FOUND,
                log_msg=True
            )

        # Pick some questions from the found IDs
        next_questions, errors = qtpm.pick_random_questions("UnrecordedQuestions", question_ids,
                                                            ["transcript"], batch_size)
        if next_questions is None:
            # return "unrec_corrupt_questions", HTTPStatus.NOT_FOUND
            return _make_err_response(
                "No valid unrecorded questions found",
                "unrec_corrupt_questions",
                HTTPStatus.NOT_FOUND
            )

        results = []
        for doc in next_questions:
            result_doc = {"id": doc["qb_id"], "transcript": doc["transcript"]}
            if "sentenceId" in doc:
                result_doc["sentenceId"] = doc["sentenceId"]
            if "tokenizations" in doc:
                result_doc["tokenizations"] = doc["tokenizations"]
            results.append(result_doc)
        return {"results": results, "errors": errors}

    def upload_questions(arguments_batch: Dict[str, List[dict]]) -> Tuple[Union[str, dict], int]:
        """
        Upload a batch of unrecorded questions.

        :param arguments_batch: A dictionary containing the list of "arguments"
        :return: A dictionary containing a "msg" stating that the upload was successful if nothing went wrong
        """
        if arguments_batch is None:
            return _make_err_response(
                "No arguments specified",
                "no_args",
                HTTPStatus.BAD_REQUEST,
                log_msg=True
            )
        arguments_list = arguments_batch["arguments"]
        _debug_variable("arguments_list", arguments_list)

        app.logger.info(f"Uploading {len(arguments_list)} unrecorded question(s)...")
        results = qtpm.unrec_questions.insert_many(arguments_list)
        app.logger.info(f"Successfully uploaded {len(results.inserted_ids)} question(s)")
        return '', HTTPStatus.OK

    @app.route("/upvote/<audio_id>", methods=["PATCH"])
    def upvote(audio_id):
        """Upvote an audio recording."""
        args = request.get_json()
        if "userId" not in args:
            return _make_err_response(
                "Argument 'userId' is undefined",
                "undefined_arg",
                HTTPStatus.BAD_REQUEST,
                log_msg=True
            )
        uid = args["userId"]

        user = qtpm.users.find_one({"_id": uid})
        rec_votes = user.get("recVotes") or []
        has_voted = False
        for i, rec_vote in enumerate(rec_votes):
            if rec_vote["id"] == audio_id:
                has_voted = True
                if rec_vote["vote"] == 1:  # Avoid upvoting twice.
                    return '', HTTPStatus.OK
                else:
                    qtpm.users.update_one({"_id": uid}, {"$set": {f"recVotes.{i}.vote": 1}})
                    qtpm.audio.update_one({"_id": audio_id}, {"$inc": {"downvotes": -1}})
                    break

        if not has_voted:
            qtpm.users.update_one({"_id": uid}, {"$push": {"recVotes": {"id": audio_id, "vote": 1}}})

        result = qtpm.audio.update_one({"_id": audio_id}, {"$inc": {"upvotes": 1}})

        if result.matched_count == 0:
            return _make_err_response(
                f"Audio document with ID '{audio_id}' not found",
                "doc_not_found",
                HTTPStatus.NOT_FOUND,
                [audio_id],
                True
            )
        return '', HTTPStatus.OK

    @app.route("/validate", methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS", "TRACE"])
    def check_token():
        """Endpoint for ORY Oathkeeper to validate a Bearer token. Accepts every standard method type because
        Oathkeeper also forwards the method type."""
        backend_names = request.args.getlist("backend")
        if backend_names:
            backend_name = _verify_backend_key(backend_names)
            return {"subject": backend_name}
        decoded = _verify_id_token()
        user_id = decoded["uid"]
        try:
            subject = qtpm.get_user_role(user_id)
        except ProfileNotFoundError as e:
            subject = "anonymous"
            app.logger.info(f"Profile not found: {e}. Subject registered as 'anonymous'")
        except MalformedProfileError as e:
            msg = str(e)
            # app.logger.error(f"{msg}. Aborting")
            # return f"{msg}\n", HTTPStatus.UNAUTHORIZED
            return _make_err_response(msg, "malformed_profile", HTTPStatus.UNAUTHORIZED, log_msg=True)

        return {"subject": subject, "extra": {}}, HTTPStatus.OK

    # DO NOT INCLUDE THE ROUTES BELOW IN DEPLOYMENT
    @app.route("/uploadtest/")
    def recording_listener_test():
        """A form for testing pre_screen. Other methods are advisable due to this method being very limited."""
        if app.config["Q_ENV"] not in [DEV_ENV_NAME, TEST_ENV_NAME]:
            abort(HTTPStatus.NOT_FOUND)
        return render_template("uploadtest.html")

    def _get_next_submission_name():
        """Return a filename safe date-timestamp of the current system time."""
        return str(datetime.now().strftime("%Y.%m.%d_%H.%M.%S.%f"))

    def _gen_secret_key(component_name):
        """
        Generate a secret key for the component name, add it to the secret keys dictionary, and return the resulting
        response.

        :param component_name: The name of the component to generate the key for
        :return: A JSON response containing either a key or an error response
        """
        if component_name in secret_keys:
            return _make_err_response(
                f"Secret key for component name '{component_name}' already exists",
                "secret_key_exists",
                HTTPStatus.UNAUTHORIZED,
                [component_name],
                True
            )
        app.logger.info(f"Generating secret key for component name '{component_name}'...")
        key = token_urlsafe(256)
        secret_keys[component_name] = key
        app.logger.info(f"Successfully generated secret key for component name '{component_name}'")
        return {"key": key}

    def _save_recording(directory: str, recording: werkzeug.datastructures.FileStorage, metadata: dict):
        """
        Write a WAV file and its JSON metadata to disk.

        :param directory: The directory that the file will be located in
        :param recording: The bytes of the audio file
        :param metadata: The metadata to save to disk
        :return: The name of the submission
        """
        app.logger.info("Saving recording...")
        submission_name = _get_next_submission_name()
        _debug_variable("submission_name", submission_name)
        submission_path = os.path.join(directory, submission_name)
        recording.save(submission_path + ".wav")
        app.logger.info("Saved recording successfully")

        app.logger.info("Writing metadata...")
        _debug_variable("metadata", metadata)
        with open(submission_path + ".json", "w") as meta_f:
            meta_f.write(bson.json_util.dumps(metadata))
        app.logger.info("Successfully wrote metadata")
        return submission_name

    def _save_recording_batch(directory: str, submissions: List[Tuple[werkzeug.datastructures.FileStorage, dict]]):
        """
        Write a batch of WAV files and their associated JSON metadata to disk.

        :param directory: The directory that the file will be located in
        :param submissions: A tuple each containing the bytes of an audio file and the associated metadata to save
        :return: The list of submission names generated in the same order as the "submissions" argument, which will each
                 have a batch number appended
        """
        app.logger.info("Saving recordings...")
        base_submission_name = _get_next_submission_name()
        _debug_variable("base_submission_name", base_submission_name)
        submission_names = []

        for i, submission in enumerate(submissions):
            recording, metadata = submission
            app.logger.info("Saving audio...")
            submission_name = f"{base_submission_name}_b{i}"
            _debug_variable("submission_name", submission_name)
            submission_path = os.path.join(directory, submission_name)
            recording.save(submission_path + ".wav")
            app.logger.info("Saved audio successfully")

            app.logger.info("Writing metadata...")
            _debug_variable("metadata", metadata)
            with open(submission_path + ".json", "w") as meta_f:
                meta_f.write(bson.json_util.dumps(metadata))
            app.logger.info("Successfully wrote metadata")
            submission_names.append(submission_name)

        return submission_names

    def _verify_id_token():
        """
        Try to decode the token if provided. If in a production environment, forbid access when the decode fails.
        This function is only callable in the context of a view function.

        :return: The result of calling firebase_admin.auth.verify_id_token() or a dictionary with a schema similar to
                 that function's output
        """
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
            app.logger.error("Could not fetch certificate. Aborting")
            decoded = None
            abort(HTTPStatus.INTERNAL_SERVER_ERROR)

        return decoded

    def _verify_backend_key(allowed_components: List[str]):
        """
        Run the logic flow for verifying a backend key, complete with logging messages. Only runnable in the context of
        a view function.

        :param allowed_components: A list of allowed backend components
        """
        _debug_variable("allowed_components", allowed_components)

        app.logger.info("Retrieving secret key from header 'Authorization'...")
        secret_key = request.headers.get("Authorization")
        component_name = request.headers.get("Backend-Name")

        _debug_variable("secret_key", secret_key, private=True)
        _debug_variable("component_name", component_name)

        prefix = "Bearer "
        if secret_key.startswith(prefix):
            secret_key = secret_key[len(prefix):]

        if component_name not in allowed_components:
            app.logger.info(f"Component '{component_name}' is not permitted to access this resource")
            abort(HTTPStatus.UNAUTHORIZED)
        if component_name not in secret_keys:
            app.logger.info(f"Secret key for component '{component_name}' not generated. Access to resource denied")
            abort(HTTPStatus.UNAUTHORIZED)
        if secret_key != secret_keys[component_name]:
            app.logger.info(f"Secret key for component '{component_name}' does not match. Access to resource denied")
            abort(HTTPStatus.UNAUTHORIZED)
        app.logger.info("Access to resource granted")
        return component_name

    def _get_private_data_string(original):
        """
        Utility function for redacting private data if ``app.config["LOG_PRIVATE_DATA"]`` is not ``True``

        :param original: The original value
        :return: The original value, or "[REDACTED]"
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
            # response = make_response(str(e))
            # response.headers["Content-Type"] = "text/plain"
            # err = (response, HTTPStatus.BAD_REQUEST)
            err = _make_err_response(
                f"Request arguments do not match schema: {e}",
                "validation_error",
                HTTPStatus.BAD_REQUEST
            )
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

    def _update_prescreen_statuses():
        """Update the status of all submissions in the status resource with the results from the queue and remove
        expired resources."""
        # Update statuses from queue.
        # TODO: Maybe do this in batches
        while True:
            try:
                result = prescreen_results_queue.get(block=False)
                _debug_variable("queue.get()", result)
            except queue.Empty:
                break
            else:
                if type(result) is tuple and isinstance(result[1], Exception):
                    for submission in result[0]:
                        ps_doc = _get_ps_doc(name=submission)
                        if ps_doc is None:
                            app.logger.debug(f"No pointer found for submission '{submission}'. Ignoring")
                            continue
                        _debug_variable("ps_doc", ps_doc)
                        ps_doc["status"] = "err"
                        ps_doc["err"] = "internal_error"
                        # ps_doc["extra"] = repr(result[1])
                else:
                    ps_doc = _get_ps_doc(name=result["name"])
                    if ps_doc is None:
                        app.logger.debug(f"No pointer found for submission '{result['name']}'. Ignoring")
                        continue
                    _debug_variable("ps_doc", ps_doc)
                    if result["case"] == "err":
                        ps_doc["status"] = "err"
                        ps_doc["err"] = result["err"]
                    else:
                        ps_doc["status"] = "finished"
                        ps_doc["accepted"] = result["case"] == "accepted"
        # Remove expired pointers.
        now = datetime.now()
        # A for loop won't delete documents properly.
        i = 0
        while i < len(prescreen_statuses):
            doc = prescreen_statuses[i]
            if now > doc["expiry"]:
                prescreen_statuses.remove(doc)
            else:
                i += 1

    def _get_ps_doc(*, name=None, pointer=None):
        """
        Get a pre-screen status document by either name or pointer. Only accepts keyword arguments.

        :param name: The submission name to look up the pre-screen status document by
        :param pointer: The pointer to look up the pre-screen status document by
        :return: The result of the lookup, or None if no result was found
        """
        k_name = None
        k_val = None
        if name:
            k_name = "name"
            k_val = name
        elif pointer:
            k_name = "pointer"
            k_val = pointer

        if k_name is None:
            raise ValueError("No arguments specified")

        for doc in prescreen_statuses:
            if doc[k_name] == k_val:
                return doc

        return None

    def _make_err_response(msg: str, id_: str, status_code: int, extra: list = None, log_msg=False):
        """
        Construct a JSON error response with the given parameters.

        :param msg: The human-readable error message to include
        :param id_: The type of error it is
        :param status_code: The HTTP status code to return
        :param extra: Extra information to include regarding the error
        :param log_msg: Whether to send the message to the log
        :return: A tuple containing the JSON response and the status code
        """
        if log_msg:
            app.logger.error(f"{msg}. Aborting")
        response = {
            "err": msg,
            "err_id": id_
        }
        if extra:
            response["extra"] = extra
        return response, status_code

    def _pick_audio(recs: list):
        weights = []
        rec_choices = []  # Segmented recordings are treated as one item
        for rec in recs:
            if "sentenceId" not in rec or rec["sentenceId"] == 0 or "tokenizationId" not in rec or rec["tokenizationId"] == 0:
                rec_choices.append(rec)

        for rec in rec_choices:
            upvotes = rec.get("upvotes") or 0
            downvotes = rec.get("downvotes") or 0

            if downvotes == 0 or upvotes == 0:
                weight = 1.0
            else:
                weight = upvotes / downvotes

            weights.append(weight)

        selected = random.choices(rec_choices, weights=weights, k=1)
        if "batchUUID" in selected[0]:  # Retrieve the other segments if it is segmented
            batch_uuid = selected[0]["batchUUID"]
            for rec in recs:
                if rec.get("batchUUID") == batch_uuid and rec != selected[0]:
                    selected.append(rec)

        selected_final = []
        for rec in selected:
            rec_final = {
                "id": rec["_id"],
                "gentleVtt": rec["gentleVtt"],
            }

            if "vtt" in rec:
                rec_final["vtt"] = rec["vtt"]
            if "sentenceId" in rec:
                rec_final["sentenceId"] = rec["sentenceId"]
            if "tokenizationId" in rec:
                rec_final["tokenizationId"] = rec["tokenizationId"]

            selected_final.append(rec_final)
        return selected_final

    return app


def main():
    parser = argparse.ArgumentParser(description="Run the data flow server", add_help=True)
    parser.add_argument("--host", dest="host", help="The host name to use for the server", default="localhost")
    parser.add_argument("--port", "-p", dest="port", type=int, help="The port to bind to", default=5000)
    args = parser.parse_args()
    app = create_app()
    app.run(host=args.host, port=args.port)


if __name__ == '__main__':
    main()
