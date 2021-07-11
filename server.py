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


class QuizzrServer:
    def __init__(self):
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

        self.database: Database = self.mongodb_client.QuizzrDatabaseDev

        self.users: Collection = self.database.Users
        self.rec_questions: Collection = self.database.RecordedQuestions
        self.unrec_questions: Collection = self.database.UnrecordedQuestions
        self.audio: Collection = self.database.Audio
        self.unproc_audio: Collection = self.database.UnprocessedAudio

        self.rec_question_ids = self.get_ids(self.rec_questions)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        self.user_ids = self.get_ids(self.users)

        self.processor = rec_processing.QuizzrProcessor(self.database, self.REC_DIR, self.meta["version"], self.SECRET_DATA_DIR, self.gdrive)

    def save_recording(self, file, metadata):
        logging.info("Saving recording...")
        submission_name = self.get_next_submission_name()
        logging.debug(f"submission_name = {submission_name}")
        submission_path = os.path.join(self.REC_DIR, submission_name)
        file.save(submission_path + ".wav")
        logging.info("Saved recording successfully")

        logging.info("Writing metadata...")
        logging.debug(f"metadata = {metadata}")
        with open(submission_path + ".json", "w") as meta_f:
            meta_f.write(bson.json_util.dumps(metadata))
        logging.info("Successfully wrote metadata")
        return submission_name

    def update_processed_audio(self, arguments: Dict[str, Any]):
        errs = []
        logging.debug(f"arguments = {arguments}")
        logging.debug("Retrieving arguments...")
        gfile_id = arguments.get("_id")
        logging.debug(f"{type(gfile_id)} _id = {gfile_id}")
        if gfile_id is None:
            logging.warning("File ID not specified in arguments. Skipping")
            errs.append(("bad_args", "undefined_gfile_id"))
            return errs

        audio_doc = self.unproc_audio.find_one({"_id": gfile_id})
        logging.debug(f"audio_doc = {audio_doc}")
        if audio_doc is None:
            logging.warning("Could not find audio document. Skipping")
            errs.append(("bad_args", "invalid_gfile_id"))
            return errs

        logging.debug("Updating audio document with results from processing...")
        proc_audio_entry = audio_doc.copy()
        proc_audio_entry.update(arguments)
        logging.debug(f"proc_audio_entry = {proc_audio_entry}")

        self.audio.insert_one(proc_audio_entry)
        self.unproc_audio.delete_one({"_id": gfile_id})

        qid = audio_doc.get("questionId")
        if qid is None:
            logging.warning("Missing question ID. Skipping")
            errs.append(("internal_error", "undefined_question_id"))
            return errs

        logging.debug("Retrieving question from unrecorded collection...")
        question = self.unrec_questions.find_one({"_id": qid})
        logging.debug(f"question = {question}")
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

    def get_gfile(self, file_id: str):
        file_request = self.gdrive.drive.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, file_request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download %d%%." % int(status.progress() * 100))
        return fh

    def find_best_audio_doc(self, recordings):
        query = {"_id": {"$in": recordings}, "version": self.meta["version"]}
        if self.audio.count_documents(query) == 0:
            logging.error("No audio documents found")
            return
        audio_cursor = self.audio.find(
            query,
            projection={"_id": 1, "vtt": 1, "score": 1},
            sort=[("score.wer", pymongo.ASCENDING)]
        )

        for audio_doc in audio_cursor:
            logging.debug(f"audio_doc = {audio_doc}")
            if audio_doc.get("vtt"):
                return audio_doc
            logging.warning("Audio document does not have VTT")

        logging.error("Failed to find a viable audio document")
        return

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

    @staticmethod
    def get_ids(collection):
        ids = []
        id_cursor = collection.find(None, {"_id": 1})
        for i, doc in enumerate(id_cursor):
            ids.append(doc["_id"])
        return ids

    @staticmethod
    def get_next_submission_name():
        return str(datetime.now().strftime("%Y.%m.%d %H.%M.%S.%f"))


qs = QuizzrServer()


def create_app():
    app = Flask(__name__)
    CORS(app)
    # TODO: multiprocessing

    @app.route("/answer/", methods=["GET"])
    def select_answer_question():
        if not qs.rec_question_ids:
            logging.error("No recorded questions found. Aborting")
            return "rec_empty_qids", HTTPStatus.INTERNAL_SERVER_ERROR

        question_ids = qs.rec_question_ids.copy()
        while True:
            next_question_id = random.choice(question_ids)
            next_question = qs.rec_questions.find_one({"_id": next_question_id})
            logging.debug(f"{type(next_question_id)} next_question_id = {next_question_id}")
            logging.debug(f"next_question = {next_question}")

            if next_question and next_question.get("recordings"):
                audio = qs.find_best_audio_doc(next_question.get("recordings"))
                if audio:
                    break
            logging.warning(f"ID {next_question_id} is invalid or associated question has no valid audio recordings")
            question_ids.remove(next_question_id)
            if not question_ids:
                logging.error("Failed to find a viable recorded question. Aborting")
                return "rec_corrupt_questions", HTTPStatus.INTERNAL_SERVER_ERROR

        result = {"vtt": audio["vtt"], "id": audio["_id"]}
        return result

    # Gets a batch of at most qs.UNPROC_FIND_LIMIT documents from the UnprocessedAudio collection in the MongoDB Atlas.
    @app.route("/audio/unprocessed/", methods=["GET"])
    def batch_unprocessed_audio():
        errs = []

        logging.info(f"Finding a batch ({qs.UNPROC_FIND_LIMIT} max) of unprocessed audio documents...")
        audio_doc_count = qs.unproc_audio.count_documents({"_id": {"$exists": True}}, limit=qs.UNPROC_FIND_LIMIT)
        if audio_doc_count == 0:
            logging.error("Could not find any audio documents")
            return "empty_unproc_audio", HTTPStatus.NOT_FOUND
        audio_cursor = qs.unproc_audio.find(limit=qs.UNPROC_FIND_LIMIT)
        qid2entries = {}
        for audio_doc in audio_cursor:
            logging.debug(f"audio_doc = {audio_doc}")
            qid = audio_doc.get("questionId")
            if qid is None:
                logging.warning("Audio document does not contain question ID")
                errs.append(("internal_error", "undefined_question_id"))
                continue
            if qid not in qid2entries:
                qid2entries[qid] = []
            qid2entries[qid].append({"_id": audio_doc["_id"]})

        qids = list(qid2entries.keys())
        logging.debug(f"qid2entries = {qid2entries}")
        logging.info(f"Found {audio_doc_count} unprocessed audio document(s)")
        if not qids:
            logging.error("No audio documents contain question IDs")
            return "empty_qid2entries", HTTPStatus.NOT_FOUND

        logging.info("Finding associated unrecorded question(s)...")
        unrec_cursor = qs.unrec_questions.find({"_id": {"$in": qids}})
        found_unrec_qids = []
        for i, question in enumerate(unrec_cursor):
            logging.debug(f"question {i} = {question}")
            qid = question["_id"]
            transcript = question.get("transcript")
            if transcript:
                entries = qid2entries[qid]
                for entry in entries:
                    entry["transcript"] = transcript
            found_unrec_qids.append(qid)
        logging.debug(f"qid2entries = {qid2entries}")
        logging.info(f"Found {len(found_unrec_qids)} unrecorded question(s)")

        found_rec_qids = [qid for qid in qids if qid not in found_unrec_qids]

        if found_rec_qids:
            logging.info(f"Finding {len(found_rec_qids)} of {len(qids)} recorded question(s)...")
            query = {"_id": {"$in": found_rec_qids}}
            rec_count = qs.rec_questions.count_documents(query)
            rec_cursor = qs.rec_questions.find(query)
            for i, question in enumerate(rec_cursor):
                logging.debug(f"question {i} = {question}")
                transcript = question.get("transcript")
                if transcript:
                    entries = qid2entries[question["_id"]]
                    for entry in entries:
                        entry["transcript"] = transcript
            logging.debug(f"qid2entries = {qid2entries}")
            logging.info(f"Found {rec_count} recorded question(s)")
        else:
            logging.info("Found all questions. Skipping finding recorded questions")

        results = []
        for entries in qid2entries.values():
            results += entries
        logging.debug(f"Final Results: {results}")
        response = {"results": results}
        if errs:
            response["errors"] = [{"type": err[0], "reason": err[1]} for err in errs]
        return response

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

    @app.route("/download/<gfile_id>", methods=["GET"])
    def send_gfile(gfile_id):
        try:
            file_data = qs.get_gfile(gfile_id)
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR
        return send_file(file_data, mimetype="audio/wav")

    @app.route("/record/", methods=["GET"])
    def select_record_question():
        if not qs.unrec_question_ids:
            logging.error("No unrecorded questions found. Aborting")
            return "unrec_empty_qids", HTTPStatus.INTERNAL_SERVER_ERROR
        question_ids = qs.unrec_question_ids.copy()
        while True:
            next_question_id = random.choice(question_ids)
            next_question = qs.unrec_questions.find_one({"_id": next_question_id})
            logging.debug(f"{type(next_question_id)} next_question_id = {next_question_id}")
            logging.debug(f"next_question = {next_question}")
            if next_question and next_question.get("transcript"):
                break
            logging.warning(f"ID {next_question_id} is invalid or associated question has no transcript")
            question_ids.remove(next_question_id)
            if not question_ids:
                logging.error("Failed to find a viable unrecorded question. Aborting")
                return "unrec_corrupt_questions", HTTPStatus.INTERNAL_SERVER_ERROR

        result = next_question["transcript"]
        return {"transcript": result, "id": str(next_question_id)}

    @app.route("/upload", methods=["POST"])
    def recording_listener():
        recording = request.files["audio"]
        # question_id = request.form["questionId"]
        # user_id = request.form["userId"]

        # qids = qs.rec_question_ids + qs.unrec_question_ids
        # if not qids:
        #     logging.error("No question IDs found in RecordedQuestions or UnrecordedQuestions. Aborting")
        #     return render_template("submission.html", status="err", err="empty_qids")

        if not qs.user_ids:
            logging.error("No user IDs found. Aborting")
            return "empty_uids", HTTPStatus.INTERNAL_SERVER_ERROR

        # question_id = random.choice(qids)
        question_id, success = error_handling.to_oid_soft(request.form.get("qid"))
        if question_id is None:
            logging.error("Form argument 'qid' is undefined. Aborting")
            return "arg_qid_undefined", HTTPStatus.BAD_REQUEST
        elif not success:
            logging.error("Form argument 'qid' is not a valid ObjectId. Aborting")
            return "arg_qid_invalid", HTTPStatus.BAD_REQUEST

        user_ids = qs.user_ids.copy()
        while True:
            user_id, success = error_handling.to_oid_soft(random.choice(user_ids))
            if success:
                break
            logging.warning(f"Found malformed user ID {user_id}. Retrying...")
            user_ids.remove(user_id)
            if not user_ids:
                logging.warning("Could not find properly formed user IDs. Proceeding with last choice")
                break

        logging.debug(f"question_id = {question_id}")
        logging.debug(f"user_id = {user_id}")

        submission_name = qs.save_recording(recording, {
            "questionId": question_id,
            "userId": user_id
        })
        logging.debug(f"submission_name = {submission_name}")
        try:
            accepted_submissions, error_submissions = qs.processor.pick_submissions(
                rec_processing.QuizzrWatcher.queue_submissions(qs.REC_DIR)
            )
        except BrokenPipeError as e:
            logging.error(f"Encountered BrokenPipeError: {e}. Aborting")
            return "broken_pipe_error", HTTPStatus.INTERNAL_SERVER_ERROR

        logging.debug(f"accepted_submissions = {accepted_submissions}")
        logging.debug(f"error_submissions = {error_submissions}")

        if submission_name in accepted_submissions:
            return {"prescreenSuccessful": True}, HTTPStatus.ACCEPTED

        if submission_name in error_submissions:
            return "error_submission", HTTPStatus.INTERNAL_SERVER_ERROR

        return {"prescreenSuccessful": False}, HTTPStatus.ACCEPTED

    @app.route("/upload/question", methods=["POST"])
    def upload_questions():
        arguments_batch = request.get_json()
        arguments_list = arguments_batch["arguments"]
        logging.debug(f"arguments_list = {arguments_list}")

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
