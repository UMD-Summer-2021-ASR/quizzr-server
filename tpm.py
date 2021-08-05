import io
import logging
import os
import random
from copy import deepcopy
from datetime import datetime
from typing import Dict, Any, List
from secrets import token_urlsafe

import pymongo
from pymongo import UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database

import firebase_admin
from firebase_admin import credentials, storage

from sv_api import QuizzrAPISpec
from sv_errors import UserExistsError


# Consists of mostly helper methods.
class QuizzrTPM:
    """"Third Party Manager"; a class containing helper methods for managing the data"""

    G_PATH_DELIMITER = "/"

    def __init__(self, database_name, app_config, secret_dir, rec_dir, api: QuizzrAPISpec):
        # self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES") or 5)
        self.app_config = app_config

        self.SECRET_DATA_DIR = secret_dir
        self.REC_DIR = rec_dir
        self.api = api

        self.mongodb_client = pymongo.MongoClient(os.environ["CONNECTION_STRING"])
        cred = credentials.Certificate(os.path.join(self.SECRET_DATA_DIR, "firebase_storage_key.json"))
        firebase_admin.initialize_app(cred, {
            "storageBucket": "quizzrio.appspot.com"
        })

        self.bucket = storage.bucket()

        self.database: Database = self.mongodb_client.get_database(database_name)

        self.users: Collection = self.database.Users
        self.rec_questions: Collection = self.database.RecordedQuestions
        self.unrec_questions: Collection = self.database.UnrecordedQuestions
        self.audio: Collection = self.database.Audio
        self.unproc_audio: Collection = self.database.UnprocessedAudio

        self.rec_question_ids = self.get_ids(self.rec_questions)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        self.user_ids = self.get_ids(self.users)

    def update_processed_audio(self, arguments: Dict[str, Any]):
        """Attach the given arguments to one unprocessed audio document and move it to the Audio collection.
        Additionally, update the recording history of the associated question and user."""
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

        rec_doc = {"id": audio_doc["_id"], "recType": audio_doc["recType"]}
        qid = audio_doc.get("questionId")

        err = self.add_rec_to_question(qid, rec_doc)
        if err:
            errs.append(err)

        user_id = audio_doc.get("userId")
        err = self.add_rec_to_user(user_id, rec_doc)
        if err:
            errs.append(err)

        return errs

    def add_rec_to_question(self, qid, rec_doc):
        if qid is None:
            logging.warning("Missing question ID. Skipping")
            return "internal_error", "undefined_question_id"
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
            question["recordings"] = [rec_doc]
            self.rec_questions.insert_one(question)
            self.unrec_questions.delete_one({"_id": qid})
        else:
            results = self.rec_questions.update_one({"_id": qid}, {"$push": {"recordings": rec_doc}})
            if results.matched_count == 0:
                logging.warning(f"Could not update question with ID {qid}")
                return "internal_error", "question_update_failure"

    def add_rec_to_user(self, user_id, rec_doc):
        """
        Push one recording document to the "recordedAudios" field of one user.

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :param rec_doc: The recording document to append. Should contain the audio ID and recording type
        :return: A tuple containing the type of error and the reason, or None if no error was encountered
        """
        logging.debug("Updating user information...")
        if user_id is None:
            logging.warning("Parameter 'user_id' is undefined. Skipping update")
            return "internal_error", "undefined_user_id"
        results = self.users.update_one({"_id": user_id}, {"$push": {"recordedAudios": rec_doc}})
        if results.matched_count == 0:
            logging.warning(f"Could not update user with ID {user_id}")
            return "internal_error", "user_update_failure"

    def add_recs_to_users(self, uid2rec_docs: Dict[str, List[dict]]):
        """
        Push multiple recording documents to the "recordedAudios" field of each user.

        :param uid2rec_docs: A dictionary mapping a user ID to the recording documents to append
        :return: An array of tuples each containing the type of error and the reason
        """
        update_batch = []
        errs = []
        logging.debug("Updating user information...")
        for user_id, rec_docs in uid2rec_docs.items():
            if user_id is None:
                logging.warning("Parameter 'user_id' is undefined. Skipping update")
                errs.append(("internal_error", "undefined_user_id"))
                continue
            for rec_doc in rec_docs:
                update_batch.append(UpdateOne({"_id": user_id}, {"$push": {"recordedAudios": rec_doc}}))
        results = self.users.bulk_write(update_batch)
        logging.info(f"Successfully updated {results.matched_count} of {len(uid2rec_docs)} user documents")
        missed_results = len(uid2rec_docs) - results.matched_count
        # if missed_results == 1:
        #     errs.append(("internal_error", "user_update_failure"))
        # elif missed_results > 1:
        #     errs.append(("internal_error", f"user_update_failure_x{missed_results}"))
        errs += [("internal_error", "user_update_failure")] * missed_results
        return errs

    def get_file_blob(self, blob_path: str):
        """Retrieve a file from Firebase Storage and store it in-memory."""
        blob = self.bucket.blob("/".join([self.app_config["BLOB_ROOT"], blob_path]))
        file_bytes = blob.download_as_bytes()
        fh = io.BytesIO(file_bytes)
        return fh

    def find_best_audio_doc(self, recordings, required_fields=None, optional_fields=None, excluded_fields=None):
        """Find and return the (processed) audio document with the best evaluation, applying a given projection."""
        query = {"_id": {"$in": [rec["id"] for rec in recordings]}, "version": self.app_config["VERSION"]}
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

    def find_questions(self, qids: list = None, **kwargs):
        """Generator function for getting questions from both collections."""
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

    # Utility methods for automatically updating the cached ID list.
    def insert_unrec_question(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.insert_one(*args, **kwargs)
        self.unrec_question_ids.append(results.inserted_id)
        return results

    def insert_unrec_questions(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.insert_many(*args, **kwargs)
        self.unrec_question_ids += results.inserted_ids
        return results

    def delete_unrec_question(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.delete_one(*args, **kwargs)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        return results

    def delete_unrec_questions(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.delete_many(*args, **kwargs)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        return results

    def insert_rec_question(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.insert_one(*args, **kwargs)
        self.rec_question_ids.append(results.inserted_id)
        return results

    def insert_rec_questions(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.insert_many(*args, **kwargs)
        self.rec_question_ids += results.inserted_ids
        return results

    def delete_rec_question(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.delete_one(*args, **kwargs)
        self.rec_question_ids = self.get_ids(self.rec_questions)
        return results

    def delete_rec_questions(self, *args, **kwargs):
        """DEPRECATED! Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.delete_many(*args, **kwargs)
        self.rec_question_ids = self.get_ids(self.rec_questions)
        return results

    def upload_many(self, file_paths: List[str], subdir) -> Dict[str, str]:
        """Upload multiple audio files to Firebase Cloud Storage."""
        # TODO: Actual BrokenPipeError handling
        file2blob = {}

        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            blob_name = token_urlsafe(self.app_config["BLOB_NAME_LENGTH"])
            blob_path = self.get_blob_path(blob_name, subdir)
            blob = self.bucket.blob(blob_path)
            blob.upload_from_filename(file_path)
            file2blob[file_name] = blob_name

        return file2blob

    def get_blob_path(self, blob_name, subdir: str):
        """Get the canonical name of a blob, which is <BLOB_ROOT>/<subdir>/<blob_name>"""
        return "/".join([self.app_config["BLOB_ROOT"], subdir, blob_name])

    def upload_one(self, file_path: str, subdir: str) -> str:
        """Upload one audio file to Firebase Cloud Storage."""
        # TODO: Actual BrokenPipeError handling
        blob_name = token_urlsafe(self.app_config["BLOB_NAME_LENGTH"])
        blob_path = self.get_blob_path(blob_name, subdir)
        blob = self.bucket.blob(blob_path)
        blob.upload_from_filename(file_path)

        return blob_name

    def mongodb_insert_submissions(
            self,
            sub2blob: Dict[str, str],
            sub2meta: Dict[str, Dict[str, Any]],
            sub2vtt):
        """Upload submission metadata to MongoDB."""
        processing_list = ["normal"]
        unproc_audio_batch = []
        audio_batch = []
        uid2rec_docs = {}
        for submission, audio_id in sub2blob.items():
            entry = {
                "_id": audio_id,
                "version": self.app_config["VERSION"]
            }
            metadata = sub2meta[submission]
            entry.update(metadata)
            if metadata["recType"] in processing_list:
                entry["gentleVtt"] = sub2vtt[submission]
            if metadata["recType"] not in processing_list:
                audio_batch.append(entry)
                if metadata["userId"] not in uid2rec_docs:
                    uid2rec_docs[metadata["userId"]] = []
                uid2rec_docs[metadata["userId"]].append({"id": audio_id, "recType": metadata["recType"]})
            else:
                unproc_audio_batch.append(entry)

        logging.debug(f"unproc_audio_batch = {unproc_audio_batch}")
        logging.debug(f"audio_batch = {audio_batch}")

        if not unproc_audio_batch:
            logging.info("No documents to insert into the UnprocessedAudio collection. Skipping")
            unproc_results = None
        else:
            unproc_results = self.unproc_audio.insert_many(unproc_audio_batch)
            logging.info(f"Inserted {len(unproc_results.inserted_ids)} document(s) into the UnprocessedAudio collection")

        if not audio_batch:
            logging.info("No documents to insert into the Audio collection. Skipping")
            proc_results = None
        else:
            proc_results = self.audio.insert_many(audio_batch)
            logging.info(f"Inserted {len(proc_results.inserted_ids)} document(s) into the Audio collection")
            self.add_recs_to_users(uid2rec_docs)
        return unproc_results, proc_results

    @staticmethod
    def get_ids(collection, query=None):
        """
        Return a list of all document IDs based on a query.

        :param collection: pymongo Collection object
        :param query: MongoDB filter argument
        :return: The _ids of every document found
        """
        ids = []
        id_cursor = collection.find(query, {"_id": 1})
        for i, doc in enumerate(id_cursor):
            ids.append(doc["_id"])
        return ids

    @staticmethod
    def get_difficulty_query_op(difficulty_limits: list, difficulty):
        """Given the list of difficulty limits and a difficulty type, return the boundaries as a MongoDB filter
        operator."""
        lower_bound = difficulty_limits[difficulty - 1] + 1 if difficulty > 0 else None
        upper_bound = difficulty_limits[difficulty]
        query_op = {}
        if lower_bound:
            query_op["$gte"] = lower_bound
        if upper_bound:
            query_op["$lte"] = upper_bound
        return query_op

    def get_profile(self, user_id: str, visibility: str):
        """
        Retrieve a User document from the associated MongoDB collection

        :param user_id: The internal ID of the User document
        :param visibility: How much of the profile to show. Valid values are "basic", "public", and "private"
        :return: A document from the MongoDB Users collection
        """
        visibility_configs = self.app_config["VISIBILITY_CONFIGS"]
        config = visibility_configs[visibility]
        return self.database.get_collection(config["collection"]).find_one(user_id, config["projection"])

    def create_profile(self, user_id: str, pfp: List[str], username: str):
        """
        Create a profile stub from the given parameters

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :param pfp: Freeform array for the profile. Potential values can be for color, the types of images to use, etc.
        :param username: The public name of the user. Must not conflict with any existing usernames
        :return: A pymongo InsertOneResult object. See documentation for further details
        :raise UserExistsError: When there is an existing user profile with the given username
        :raise pymongo.errors.DuplicateKeyError:
        """
        if self.users.find_one({"username": username}) is not None:
            raise UserExistsError(username)
        profile_stub = self.api.get_schema_stub("User")
        profile_stub["_id"] = user_id
        profile_stub["pfp"] = pfp
        profile_stub["username"] = username
        profile_stub["creationDate"] = datetime.now().isoformat()
        return self.users.insert_one(profile_stub)

    def modify_profile(self, user_id: str, update_args: Dict[str, Any]):
        """
        Modify a user profile

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :param update_args: The fields to replace and their corresponding values
        :return: A pymongo UpdateResult object. See documentation for further details
        """
        username = update_args.get("username")
        if username and self.users.find_one({"username": username}) is not None:
            raise UserExistsError(username)
        return self.users.update_one({"_id": user_id}, {"$set": update_args})

    def delete_profile(self, user_id):
        """
        Delete a user profile

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :return: A pymongo DeleteResult object. See documentation for further details
        """
        return self.users.delete_one({"_id": user_id})
