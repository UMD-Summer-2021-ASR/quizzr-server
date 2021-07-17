import io
import json
import logging
import os
import random
from copy import deepcopy
from typing import Dict, Any, List

import pymongo
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from pymongo.collection import Collection
from pymongo.database import Database

import gdrive_authentication


# Consists of mostly helper methods.
class QuizzrTPM:
    def __init__(self, database_name="QuizzrDatabase", g_folder_name="Recordings", server_dir=os.path.dirname(__file__), version="0.0.0", folder_id=None):
        # self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES") or 5)
        self.VERSION = version

        # self.SERVER_DIR = os.environ['SERVER_DIR']
        self.SERVER_DIR = server_dir
        self.SECRET_DATA_DIR = os.path.join(self.SERVER_DIR, "privatedata")
        self.REC_DIR = os.path.join(self.SERVER_DIR, "recordings")
        with open(os.path.join(self.SERVER_DIR, "metadata.json")) as meta_f:
            self.meta = json.load(meta_f)

        # with open(os.path.join(self.SECRET_DATA_DIR, "connectionstring")) as f:
        #     self.mongodb_client = pymongo.MongoClient(f.read())
        self.mongodb_client = pymongo.MongoClient(os.environ["CONNECTION_STRING"])
        self.gdrive = gdrive_authentication.GDriveAuth(self.SECRET_DATA_DIR)

        cache_file_name = ".tpm_cache.json"
        cache_file_path = os.path.join(self.SERVER_DIR, cache_file_name)
        cached_ids = {}

        # self.folder_id initialization
        # Try to use override first
        self.folder_id = folder_id

        # If override not defined, try looking in the cached IDs list
        if not self.folder_id and os.path.exists(cache_file_path):
            with open(cache_file_path, "r") as cache_f:
                cached_ids = json.load(cache_f)
            self.folder_id = cached_ids.get(g_folder_name)

        # If no results were found from looking in the cached IDs list or there is no cached IDs list, create a new ID
        if not self.folder_id:
            self.folder_id = self.create_g_folder(g_folder_name)

        # TODO: Make it only write if there is no cached IDs or the list of cached IDs changed
        if not folder_id:
            cached_ids[g_folder_name] = self.folder_id
            with open(cache_file_path, "w") as cache_f:
                json.dump(cached_ids, cache_f)

        self.database: Database = self.mongodb_client.get_database(database_name)

        self.users: Collection = self.database.Users
        self.rec_questions: Collection = self.database.RecordedQuestions
        self.unrec_questions: Collection = self.database.UnrecordedQuestions
        self.audio: Collection = self.database.Audio
        self.unproc_audio: Collection = self.database.UnprocessedAudio

        self.rec_question_ids = self.get_ids(self.rec_questions)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        self.user_ids = self.get_ids(self.users)

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

    # Generator function for getting questions from both collections.
    def find_questions(self, qids: list = None, **kwargs):
        kwargs_c = deepcopy(kwargs)

        # Overrides _id argument in filter.
        if qids:
            if "filter" not in kwargs_c:
                kwargs_c["filter"] = {}
            kwargs_c["filter"]["_id"] = {"$in": qids}

        logging.info("Finding unrecorded questions...")
        unrec_cursor = self.unrec_questions.find(**kwargs_c)
        found_unrec_qids = []
        for i, question in enumerate(unrec_cursor):
            logging.debug(f"question {i} = {question!r}")
            found_unrec_qids.append(question["_id"])
            yield question
        logging.info(f"Found {len(found_unrec_qids)} unrecorded question(s)")

        if qids:
            rec_qids = [qid for qid in qids if qid not in found_unrec_qids]
            if not rec_qids:
                logging.info("Found all questions. Skipping finding recorded questions")
                return
            kwargs_c["filter"]["_id"] = {"$in": rec_qids}
            logging.info(f"Finding {len(rec_qids)} of {len(qids)} recorded question(s)...")
        else:
            logging.info("Finding recorded questions...")

        rec_count = self.rec_questions.count_documents(**kwargs_c)
        rec_cursor = self.rec_questions.find(**kwargs_c)
        for i, question in enumerate(rec_cursor):
            logging.debug(f"question {i} = {question!r}")
            yield question
        logging.info(f"Found {rec_count} recorded question(s)")

    def pick_random_question(self, question_ids):
        while question_ids:
            next_question_id = random.choice(question_ids)
            next_question = self.unrec_questions.find_one({"_id": next_question_id})
            logging.debug(f"{type(next_question_id)} next_question_id = {next_question_id!r}")
            logging.debug(f"next_question = {next_question!r}")
            if next_question and "transcript" in next_question:
                return next_question
            logging.warning(f"ID {next_question_id} is invalid or associated question has no transcript")
            question_ids.remove(next_question_id)
        logging.error("Failed to find a viable unrecorded question. Aborting")

    def pick_random_questions(self, question_ids, batch_size):
        qids_pool = question_ids.copy()
        random.shuffle(qids_pool)
        next_batch_size = batch_size
        questions = []
        errors = []
        while qids_pool:
            if len(qids_pool) >= next_batch_size:
                next_id_batch = qids_pool[:next_batch_size]
                qids_pool = qids_pool[next_batch_size:]
            else:
                next_id_batch = qids_pool.copy()
                qids_pool = []
            logging.debug(f"next_id_batch = {next_id_batch!r}")
            logging.debug(f"qids_pool = {qids_pool!r}")
            questions_cursor = self.unrec_questions.find({"_id": {"$in": next_id_batch}})
            for doc in questions_cursor:
                logging.debug(f"doc = {doc!r}")
                if "transcript" in doc:
                    questions.append(doc)
                else:
                    logging.warning("Question does not contain required field 'transcript'. Ignoring")
                    errors.append((doc, "missing_transcript"))
            next_batch_size -= len(questions)
            if next_batch_size == 0:
                logging.info("Found all questions requested. Returning results")
                return questions, errors
            logging.info(f"Found {len(questions)} of {batch_size} questions requested. Searching for {next_batch_size} more...")
        if not qids_pool:
            logging.info("Could not find any more valid questions. Returning results")
        if questions:
            return questions, errors
        logging.error("Failed to find any viable unrecorded questions. Aborting")

    # Utility methods for automatically updating the cached ID list. Deprecated.
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

    def create_g_folder(self, name, parents=None):
        file_metadata = {
            "name": name,
            "mimeType": "application/vnd.google-apps.folder"
        }
        if parents:
            file_metadata["parents"] = parents
        folder = self.gdrive.drive.files().create(body=file_metadata, fields='id').execute()
        return folder.get('id')

    # Upload multiple submissions to Google Drive.
    def gdrive_upload_many(self, file_paths: List[str]) -> Dict[str, str]:
        # TODO: Actual BrokenPipeError handling
        file2gfid = {}
        if self.gdrive.creds.expired:
            self.gdrive.refresh()

        # TODO: Convert to batch request
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            file_metadata = {"name": file_name, "parents": [self.folder_id]}

            media = MediaFileUpload(file_path, mimetype="audio/wav")
            gfile = self.gdrive.drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
            gfile_id = gfile.get("id")
            file2gfid[file_name] = gfile_id
        return file2gfid

    # Upload submission metadata to MongoDB.
    def mongodb_insert_submissions(
            self,
            sub2gfid: Dict[str, str],
            sub2meta: Dict[str, Dict[str, Any]],
            sub2vtt):
        mongodb_insert_batch = []
        for submission in sub2gfid.keys():
            entry = {
                "_id": sub2gfid[submission],
                "version": self.VERSION,
                "gentleVtt": sub2vtt[submission]
            }
            entry.update(sub2meta[submission])
            mongodb_insert_batch.append(entry)
        if not mongodb_insert_batch:
            logging.warning("No documents to insert. Skipping")
            return
        results = self.unproc_audio.insert_many(mongodb_insert_batch)
        logging.info(f"Inserted {len(results.inserted_ids)} document(s) into the UnprocessedAudio collection")
        return results

    # Return a list of all document IDs based on a query.
    @staticmethod
    def get_ids(collection, query=None):
        ids = []
        id_cursor = collection.find(query, {"_id": 1})
        for i, doc in enumerate(id_cursor):
            ids.append(doc["_id"])
        return ids

    @staticmethod
    def get_difficulty_query_op(difficulty_limits: list, difficulty):
        lower_bound = difficulty_limits[difficulty - 1] + 1 if difficulty > 0 else None
        upper_bound = difficulty_limits[difficulty]
        query_op = {}
        if lower_bound:
            query_op["$gte"] = lower_bound
        if upper_bound:
            query_op["$lte"] = upper_bound
        return query_op
