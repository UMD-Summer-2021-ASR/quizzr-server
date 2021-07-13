from datetime import datetime
import io
import json
import logging
import os
import random
from http import HTTPStatus
from typing import Dict, Any

import bson.json_util
from flask import Flask, request, render_template, send_file
import pymongo
from flask_cors import CORS
from googleapiclient.http import MediaIoBaseDownload
from pymongo.collection import Collection
from pymongo.database import Database

import error_handling
import gdrive_authentication
import rec_processing

logging.basicConfig(level=os.environ.get("QUIZZR_LOG") or "DEBUG")
logger = logging.getLogger(__name__)

# TODO: Implement rec_processing module through multiprocessing


# Third party management
# TODO: Migrate to tpm.py. Rename to "QuizzrTPM" for "Quizzr Third Party Manager".
class QuizzrServer:
    def __init__(self, database_name="QuizzrDatabase"):
        self.UNPROC_FIND_LIMIT = int(os.environ.get("UNPROC_FIND_LIMIT") or 32)  # Arbitrary default
        self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES") or 5)

        # self.SERVER_DIR = os.environ['SERVER_DIR']
        self.SERVER_DIR = os.path.dirname(__file__)
        self.SECRET_DATA_DIR = os.path.join(self.SERVER_DIR, "privatedata")
        self.REC_DIR = os.path.join(self.SERVER_DIR, "recordings")
        with open(os.path.join(self.SERVER_DIR, "metadata.json")) as meta_f:
            self.meta = json.load(meta_f)

        # with open(os.path.join(self.SECRET_DATA_DIR, "connectionstring")) as f:
        #     self.mongodb_client = pymongo.MongoClient(f.read())
        self.mongodb_client = pymongo.MongoClient(os.environ["CONNECTION_STRING"])
        self.gdrive = gdrive_authentication.GDriveAuth(self.SECRET_DATA_DIR)

        self.database: Database = self.mongodb_client.get_database(database_name)

        self.users: Collection = self.database.Users
        self.rec_questions: Collection = self.database.RecordedQuestions
        self.unrec_questions: Collection = self.database.UnrecordedQuestions
        self.audio: Collection = self.database.Audio
        self.unproc_audio: Collection = self.database.UnprocessedAudio

        self.rec_question_ids = self.get_ids(self.rec_questions)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        self.user_ids = self.get_ids(self.users)

        self.processor = rec_processing.QuizzrProcessor(self.database, self.REC_DIR, self.meta["version"], self.gdrive)

    # *** Will not be part of the QuizzrTPM class
    # Write a WAV file and its JSON metadata to disk.
    def save_recording(self, file, metadata):
        logging.info("Saving recording...")
        submission_name = self.get_next_submission_name()
        logging.debug(f"submission_name = {submission_name!r}")
        submission_path = os.path.join(self.REC_DIR, submission_name)
        file.save(submission_path + ".wav")
        logging.info("Saved recording successfully")

        logging.info("Writing metadata...")
        logging.debug(f"metadata = {metadata!r}")
        with open(submission_path + ".json", "w") as meta_f:
            meta_f.write(bson.json_util.dumps(metadata))
        logging.info("Successfully wrote metadata")
        return submission_name

    # Attach the given arguments to one unprocessed audio document and move it to the Audio collection.
    # Additionally, update the recording history of the associated question and user.
    def update_processed_audio(self, arguments: Dict[str, Any]):
        errs = []
        logging.debug(f"arguments = {arguments!r}")
        logging.debug("Retrieving arguments...")
        gfile_id = arguments.get("_id")
        logging.debug(f"{type(gfile_id)} _id = {gfile_id!r}")
        if gfile_id is None:
            logging.warning("File ID not specified in arguments. Skipping")
            errs.append(("bad_args", "undefined_gfile_id"))
            return errs

        audio_doc = self.unproc_audio.find_one({"_id": gfile_id})
        logging.debug(f"audio_doc = {audio_doc!r}")
        if audio_doc is None:
            logging.warning("Could not find audio document. Skipping")
            errs.append(("bad_args", "invalid_gfile_id"))
            return errs

        logging.debug("Updating audio document with results from processing...")
        proc_audio_entry = audio_doc.copy()
        proc_audio_entry.update(arguments)
        logging.debug(f"proc_audio_entry = {proc_audio_entry!r}")

        self.audio.insert_one(proc_audio_entry)
        self.unproc_audio.delete_one({"_id": gfile_id})

        qid = audio_doc.get("questionId")
        if qid is None:
            logging.warning("Missing question ID. Skipping")
            errs.append(("internal_error", "undefined_question_id"))
            return errs

        logging.debug("Retrieving question from unrecorded collection...")
        question = self.unrec_questions.find_one({"_id": qid})
        logging.debug(f"question = {question!r}")
        unrecorded = question is not None
        if unrecorded:
            logging.debug("Unrecorded question not found")
        else:
            logging.debug("Found unrecorded question")

        logging.debug("Updating question...")
        if unrecorded:
            question["recordings"] = [gfile_id]
            self.insert_rec_question(question)
            self.delete_unrec_question({"_id": qid})
        else:
            results = self.rec_questions.update_one({"_id": qid}, {"$push": {"recordings": gfile_id}})
            if results.matched_count == 0:
                logging.warning(f"Could not update question with ID {qid}")
                errs.append(("internal_error", "question_update_failure"))

        logging.debug("Updating user information...")
        user_id = audio_doc.get("userId")
        if user_id is None:
            logging.warning("Audio document does not contain user ID. Skipping update")
            errs.append(("internal_error", "undefined_user_id"))
            return errs
        results = self.users.update_one({"_id": user_id}, {"$push": {"recordedAudios": gfile_id}})
        if results.matched_count == 0:
            logging.warning(f"Could not update user with ID {user_id}")
            errs.append(("internal_error", "user_update_failure"))

        return errs

    # Retrieve a file from Google Drive and stores it in-memory.
    def get_gfile(self, file_id: str):
        file_request = self.gdrive.drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, file_request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))
        return fh

    # Find and return the (processed) audio document with the best evaluation, applying a given projection.
    def find_best_audio_doc(self, recordings, required_fields=None, optional_fields=None, excluded_fields=None):
        query = {"_id": {"$in": recordings}, "version": self.meta["version"]}
        if self.audio.count_documents(query) == 0:
            logging.error("No audio documents found")
            return

        projection = {}
        if required_fields:
            for field in required_fields:
                projection[field] = 1
        if optional_fields:
            for field in optional_fields:
                projection[field] = 1
        if excluded_fields:
            for field in excluded_fields:
                projection[field] = 0

        audio_cursor = self.audio.find(
            query,
            projection=projection,
            sort=[("score.wer", pymongo.ASCENDING)]
        )

        for audio_doc in audio_cursor:
            logging.debug(f"audio_doc = {audio_doc!r}")
            if all(field in audio_doc for field in required_fields):
                return audio_doc
            logging.warning(f"Audio document is missing at least one required field: {', '.join(required_fields)}. Skipping")

        logging.error("Failed to find a viable audio document")
        return

    # Utility methods for automatically updating the cached ID list.
    def insert_unrec_question(self, *args, **kwargs):
        results = self.unrec_questions.insert_one(*args, **kwargs)
        self.unrec_question_ids.append(results.inserted_id)
        return results

    def insert_unrec_questions(self, *args, **kwargs):
        results = self.unrec_questions.insert_many(*args, **kwargs)
        self.unrec_question_ids += results.inserted_ids
        return results

    def delete_unrec_question(self, *args, **kwargs):
        results = self.unrec_questions.delete_one(*args, **kwargs)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        return results

    def delete_unrec_questions(self, *args, **kwargs):
        results = self.unrec_questions.delete_many(*args, **kwargs)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        return results

    def insert_rec_question(self, *args, **kwargs):
        results = self.rec_questions.insert_one(*args, **kwargs)
        self.rec_question_ids.append(results.inserted_id)
        return results

    def insert_rec_questions(self, *args, **kwargs):
        results = self.rec_questions.insert_many(*args, **kwargs)
        self.rec_question_ids += results.inserted_ids
        return results

    def delete_rec_question(self, *args, **kwargs):
        results = self.rec_questions.delete_one(*args, **kwargs)
        self.rec_question_ids = self.get_ids(self.rec_questions)
        return results

    def delete_rec_questions(self, *args, **kwargs):
        results = self.rec_questions.delete_many(*args, **kwargs)
        self.rec_question_ids = self.get_ids(self.rec_questions)
        return results

    # Generator function for getting questions from both collections.
    def find_questions(self, qids=None):
        # TODO: Allow for other arguments
        query = {}
        # kwargs_c = deepcopy(kwargs)

        # Any additional specifications on the query.
        # query_arg = kwargs.get("query")
        # if query_arg is not None:
        #     query.update(query_arg)  # Overrides

        # Overrides _id argument in query.
        if qids is not None:
            query["_id"] = {"$in": qids}

        logging.info("Finding unrecorded question(s)...")
        unrec_cursor = self.unrec_questions.find(query)
        found_unrec_qids = []
        for i, question in enumerate(unrec_cursor):
            logging.debug(f"question {i} = {question!r}")
            found_unrec_qids.append(question["_id"])
            yield question
        logging.info(f"Found {len(found_unrec_qids)} unrecorded question(s)")

        found_rec_qids = [qid for qid in qids if qid not in found_unrec_qids]

        if found_rec_qids:
            logging.info(f"Finding {len(found_rec_qids)} of {len(qids)} recorded question(s)...")
            query = {"_id": {"$in": found_rec_qids}}
            rec_count = self.rec_questions.count_documents(query)
            rec_cursor = self.rec_questions.find(query)
            for i, question in enumerate(rec_cursor):
                logging.debug(f"question {i} = {question!r}")
                yield question
            logging.info(f"Found {rec_count} recorded question(s)")
        else:
            logging.info("Found all questions. Skipping finding recorded questions")

    # Return a list of all document IDs based on a query.
    @staticmethod
    def get_ids(collection, query=None):
        ids = []
        id_cursor = collection.find(query, {"_id": 1})
        for i, doc in enumerate(id_cursor):
            ids.append(doc["_id"])
        return ids

    # *** Will not be part of the QuizzrTPM class
    @staticmethod
    def get_next_submission_name():
        return str(datetime.now().strftime("%Y.%m.%d %H.%M.%S.%f"))


# TODO: arguments: "env" (default "production"), "env_overrides"
# TODO: .server_cache.json (for storing folder IDs).
def create_app(database_name: str = None):
    app = Flask(__name__)
    CORS(app)
    qs = QuizzrServer(database_name or os.environ.get("DATABASE"))
    # TODO: multiprocessing

    # Find a random recorded question and return the VTT and ID of the recording with the best evaluation.
    @app.route("/answer/", methods=["GET"])
    def select_answer_question():
        question_ids = qs.get_ids(qs.rec_questions)
        if not question_ids:
            logging.error("No recorded questions found. Aborting")
            return "rec_empty_qids", HTTPStatus.NOT_FOUND

        # question_ids = qs.rec_question_ids.copy()
        while True:
            next_question_id = random.choice(question_ids)
            next_question = qs.rec_questions.find_one({"_id": next_question_id})
            logging.debug(f"{type(next_question_id)} next_question_id = {next_question_id!r}")
            logging.debug(f"next_question = {next_question!r}")

            if next_question and next_question.get("recordings"):
                audio = qs.find_best_audio_doc(
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

        # result = {"vtt": audio["vtt"], "gentleVtt": audio["gentleVtt"], "id": audio["_id"]}
        # result = audio.copy()
        # del result["score"]
        return audio

    # Get a batch of at most qs.UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB Atlas.
    @app.route("/audio/unprocessed/", methods=["GET"])
    def batch_unprocessed_audio():
        errs = []
        results_projection = ["_id", "diarMetadata"]  # In addition to the transcript

        logging.info(f"Finding a batch ({qs.UNPROC_FIND_LIMIT} max) of unprocessed audio documents...")
        audio_doc_count = qs.unproc_audio.count_documents({"_id": {"$exists": True}}, limit=qs.UNPROC_FIND_LIMIT)
        if audio_doc_count == 0:
            logging.error("Could not find any audio documents")
            return "empty_unproc_audio", HTTPStatus.NOT_FOUND
        audio_cursor = qs.unproc_audio.find(limit=qs.UNPROC_FIND_LIMIT)
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

        questions = qs.find_questions(qids)
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
    def processed_audio():
        arguments_batch = request.get_json()
        arguments_list = arguments_batch.get("arguments")
        if arguments_list is None:
            return "undefined_arguments", HTTPStatus.BAD_REQUEST
        logging.info(f"Updating data related to {len(arguments_list)} audio documents...")
        errors = []
        success_count = 0
        for arguments in arguments_list:
            errs = qs.update_processed_audio(arguments)
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

    # Retrieve a file from Google Drive.
    @app.route("/download/<gfile_id>", methods=["GET"])
    def send_gfile(gfile_id):
        if qs.gdrive.creds.expired:
            qs.gdrive.refresh()
        try:
            file_data = qs.get_gfile(gfile_id)
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR
        return send_file(file_data, mimetype="audio/wav")

    # Find a random unrecorded question and return the ID and transcript.
    @app.route("/record/", methods=["GET"])
    def select_record_question():
        question_ids = qs.get_ids(qs.unrec_questions)
        if not question_ids:
            logging.error("No unrecorded questions found. Aborting")
            return "unrec_empty_qids", HTTPStatus.NOT_FOUND

        # question_ids = qs.unrec_question_ids.copy()
        while True:
            next_question_id = random.choice(question_ids)
            next_question = qs.unrec_questions.find_one({"_id": next_question_id})
            logging.debug(f"{type(next_question_id)} next_question_id = {next_question_id!r}")
            logging.debug(f"next_question = {next_question!r}")
            if next_question and next_question.get("transcript"):
                break
            logging.warning(f"ID {next_question_id} is invalid or associated question has no transcript")
            question_ids.remove(next_question_id)
            if not question_ids:
                logging.error("Failed to find a viable unrecorded question. Aborting")
                return "unrec_corrupt_questions", HTTPStatus.NOT_FOUND

        result = next_question["transcript"]
        return {"transcript": result, "id": str(next_question_id)}

    # Submit a recording for pre-screening and upload it to the database if it passes.
    @app.route("/upload", methods=["POST"])
    def recording_listener():
        recording = request.files.get("audio")
        question_id = request.form.get("qid")
        diarization_metadata = request.form.get("diarMetadata")

        logging.debug(f"question_id = {question_id!r}")
        logging.debug(f"diarization_metadata = {diarization_metadata!r}")

        if recording is None:
            logging.error("No audio recording defined. Aborting")
            return "arg_audio_undefined", HTTPStatus.BAD_REQUEST
        question_id, success = error_handling.to_oid_soft(question_id)
        if question_id is None:
            logging.error("Form argument 'qid' is undefined. Aborting")
            return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
        elif not success:
            logging.error("Form argument 'qid' is not a valid ObjectId. Aborting")
            return "arg_qid_invalid", HTTPStatus.BAD_REQUEST

        user_ids = qs.get_ids(qs.users)
        if not user_ids:
            logging.error("No user IDs found. Aborting")
            return "empty_uids", HTTPStatus.INTERNAL_SERVER_ERROR
        # user_ids = qs.user_ids.copy()
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
            "questionId": question_id,
            "userId": user_id
        }
        if diarization_metadata:
            metadata["diarMetadata"] = diarization_metadata

        submission_name = qs.save_recording(recording, metadata)
        logging.debug(f"submission_name = {submission_name!r}")
        try:
            accepted_submissions, errors = qs.processor.pick_submissions(
                rec_processing.QuizzrWatcher.queue_submissions(qs.REC_DIR)
            )
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR

        logging.debug(f"accepted_submissions = {accepted_submissions!r}")
        logging.debug(f"errors = {errors!r}")

        if submission_name in accepted_submissions:
            return {"prescreenSuccessful": True}, HTTPStatus.ACCEPTED

        if submission_name in errors:
            return errors[submission_name], HTTPStatus.INTERNAL_SERVER_ERROR

        return {"prescreenSuccessful": False}, HTTPStatus.ACCEPTED

    # Upload a batch of unrecorded questions.
    @app.route("/upload/question", methods=["POST"])
    def upload_questions():
        arguments_batch = request.get_json()
        arguments_list = arguments_batch["arguments"]
        logging.debug(f"arguments_list = {arguments_list!r}")

        logging.info(f"Uploading {len(arguments_list)} unrecorded questions...")
        qs.insert_unrec_questions(arguments_list)
        logging.info("Successfully uploaded questions")
        return {"msg": "unrec_question.upload_success"}

    # DO NOT INCLUDE THE ROUTES BELOW IN DEPLOYMENT
    @app.route("/uploadtest/")
    def recording_listener_test():
        return render_template("uploadtest.html")

    @app.route("/processedaudiotest/")
    def processed_audio_test():
        return render_template("processedaudiotest.html")

    return app
