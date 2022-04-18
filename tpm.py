import io
import logging
import os
import pprint
import random
from copy import deepcopy
from datetime import datetime
from itertools import chain
from typing import Dict, Any, List, Tuple, Optional, Union
# from secrets import token_urlsafe
from uuid import uuid4

import pymongo
from pymongo import UpdateOne
from pymongo.collection import Collection
from pymongo.database import Database

import firebase_admin
from firebase_admin import credentials, storage
from pymongo.results import InsertManyResult

from sv_errors import UsernameTakenError, ProfileNotFoundError, MalformedProfileError


# Consists of mostly helper methods.
class QuizzrTPM:
    """"Third Party Manager"; a class containing helper methods for managing the data"""

    G_PATH_DELIMITER = "/"

    def __init__(self, database_name: str, config: dict,
                 firebase_app_specifier: Union[str, firebase_admin.App], logger=None):
        """
        Create a MongoClient and initialize a Firebase app, or use an existing one if provided.

        :param database_name: The name of the MongoDB database to use
        :param config: The configuration to use
        :param firebase_app_specifier: A string specifying the path to the service account key or a Firebase app
        """
        # self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES") or 5)
        self.logger = logger or logging.getLogger(__name__)
        self.config = config

        self.mongodb_client = pymongo.MongoClient(os.environ["CONNECTION_STRING"])
        if type(firebase_app_specifier) is str:
            cred = credentials.Certificate(firebase_app_specifier)
            self.app = firebase_admin.initialize_app(cred, {
                "storageBucket": "quizzrio.appspot.com"
            })

        self.bucket = storage.bucket()

        self.database: Database = self.mongodb_client.get_database(database_name)

        self.users: Collection = self.database.Users
        self.rec_questions: Collection = self.database.RecordedQuestions
        self.unrec_questions: Collection = self.database.UnrecordedQuestions
        self.audio: Collection = self.database.Audio
        self.unproc_audio: Collection = self.database.UnprocessedAudio
        self.games: Collection = self.database.Games

        self.rec_question_ids = self.get_ids(self.rec_questions)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        self.user_ids = self.get_ids(self.users)

    def update_processed_audio(self, arguments: Dict[str, Any]) -> List[Tuple[str, str]]:
        """
        Attach the given arguments to one unprocessed audio document and move it to the Audio collection. Additionally,
        update the recording history of the associated question and user.

        :param arguments: The _id of the audio document to update along with the fields to add to it
        :return: A list of tuples each containing an error type and the cause.
        """
        errs = []
        self._debug_variable("arguments", arguments)
        self.logger.debug("Retrieving arguments...")
        blob_name = arguments.get("_id")
        self._debug_variable("blob_name", blob_name, include_type=True)
        if blob_name is None:
            self.logger.warning("File ID not specified in arguments. Skipping")
            errs.append(("bad_args", "undefined_blob_name"))
            return errs

        audio_doc = self.unproc_audio.find_one({"_id": blob_name})
        self._debug_variable("audio_doc", audio_doc)
        if audio_doc is None:
            self.logger.warning("Could not find audio document. Skipping")
            errs.append(("bad_args", "invalid_blob_name"))
            return errs

        self.logger.debug("Updating audio document with results from processing...")
        proc_audio_entry = audio_doc.copy()
        proc_audio_entry.update(arguments)
        self._debug_variable("proc_audio_entry", proc_audio_entry)

        self.audio.insert_one(proc_audio_entry)
        self.unproc_audio.delete_one({"_id": blob_name})

        # TODO: Embed difficulty type in document
        qid = audio_doc.get("qb_id")
        sid = audio_doc.get("sentenceId")

        rec_doc = {"id": audio_doc["_id"], "recType": audio_doc["recType"]}

        err = self.add_rec_to_question(qid, rec_doc, sid)
        if err:
            errs.append(err)

        user_id = audio_doc.get("userId")
        err = self.add_rec_to_user(user_id, rec_doc)
        if err:
            errs.append(err)

        return errs

    def add_rec_to_question(self, qid: int, rec_doc: dict, sid: int = None) -> Tuple[str, str]:
        """
        Append a recording document to the ``"recordedAudios"`` field of a question

        :param qid: The ID of the question
        :param rec_doc: The recording document to append. Should contain the audio ID and recording type
        :param sid: (optional) The ID of the sentence. Use for segmented questions.
        :return: A tuple containing the error type and reason, or None if no error occurred.
        """
        if qid is None:
            self.logger.warning("Missing question ID. Skipping")
            return "internal_error", "undefined_question_id"
        query = {"qb_id": qid}

        if sid is not None:
            query["sentenceId"] = sid

        self.logger.debug("Retrieving question from unrecorded collection...")
        question = self.unrec_questions.find_one(query)
        self._debug_variable("question", question)
        unrecorded = question is not None
        if unrecorded:
            self.logger.debug("Unrecorded question not found")
        else:
            self.logger.debug("Found unrecorded question")

        self.logger.debug("Updating question...")
        if unrecorded:
            question["recordings"] = [rec_doc]
            self.rec_questions.insert_one(question)
            self.unrec_questions.delete_one(query)
        else:
            results = self.rec_questions.update_one(query, {"$push": {"recordings": rec_doc}})
            if results.matched_count == 0:
                self.logger.warning(f"Could not update question with ID {qid}")
                return "internal_error", "question_update_failure"

    def add_recs_to_questions(self, qid2rec_docs: Dict[int, List[dict]]):
        """
        Push multiple recording documents to the ``"recordedAudios"`` field of each user.

        :param qid2rec_docs: A dictionary mapping a question ID to the recording documents to append
        :return: An array of tuples each containing the type of error and the reason
        """
        errs = []

        for qid, rec_docs in qid2rec_docs.items():
            for rec_doc in rec_docs:
                err = self.add_rec_to_question(qid, rec_doc)
                if err:
                    errs.append(err)

        return errs

    def add_rec_to_user(self, user_id: str, rec_doc: dict) -> Optional[Tuple[str, str]]:
        """
        Push one recording document to the ``"recordedAudios"`` field of one user.

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :param rec_doc: The recording document to append. Should contain the audio ID and recording type
        :return: A tuple containing the type of error and the reason, or None if no error was encountered
        """
        self.logger.debug("Updating user information...")
        if user_id is None:
            self.logger.warning("Parameter 'user_id' is undefined. Skipping update")
            return "internal_error", "undefined_user_id"
        results = self.users.update_one({"_id": user_id}, {"$push": {"recordedAudios": rec_doc}})
        if results.matched_count == 0:
            self.logger.warning(f"Could not update user with ID {user_id}")
            return "internal_error", "user_update_failure"

    def add_recs_to_users(self, uid2rec_docs: Dict[str, List[dict]]):
        """
        Push multiple recording documents to the ``"recordedAudios"`` field of each user.

        :param uid2rec_docs: A dictionary mapping a user ID to the recording documents to append
        :return: An array of tuples each containing the type of error and the reason
        """
        update_batch = []
        errs = []
        self.logger.debug("Updating user information...")
        for user_id, rec_docs in uid2rec_docs.items():
            if user_id is None:
                self.logger.warning("Parameter 'user_id' is undefined. Skipping update")
                errs.append(("internal_error", "undefined_user_id"))
                continue
            for rec_doc in rec_docs:
                update_batch.append(UpdateOne({"_id": user_id}, {"$push": {"recordedAudios": rec_doc}}))
        results = self.users.bulk_write(update_batch)
        # FIXME: Message does not count the number of user documents updated, rather the number of successful updates.
        self.logger.info(f"Successfully updated {results.matched_count} of {len(uid2rec_docs)} user documents")
        missed_results = len(uid2rec_docs) - results.matched_count
        # if missed_results == 1:
        #     errs.append(("internal_error", "user_update_failure"))
        # elif missed_results > 1:
        #     errs.append(("internal_error", f"user_update_failure_x{missed_results}"))
        errs += [("internal_error", "user_update_failure")] * missed_results
        return errs

    def get_file_blob(self, blob_path: str):
        """
        Retrieve a file from Firebase Storage and store it in-memory.

        :param blob_path: The canonical blob name
        :return: An in-memory bytes buffer handler for the file
        """
        blob_name = "/".join([self.config["BLOB_ROOT"], blob_path])
        self._debug_variable("blob_name", blob_name)
        blob = self.bucket.blob(blob_name)
        file_bytes = blob.download_as_bytes()
        fh = io.BytesIO(file_bytes)
        return fh

    def delete_file_blob(self, blob_path: str):
        blob_name = "/".join([self.config["BLOB_ROOT"], blob_path])
        self._debug_variable("blob_name", blob_name)
        blob = self.bucket.blob(blob_name)
        blob.delete()

    def find_best_audio_doc(self,
                            id_list: List[str],
                            required_fields: List[str] = None,
                            optional_fields: List[str] = None,
                            excluded_fields: List[str] = None) -> Optional[dict]:
        """
        Find and return the (processed) audio document with the best evaluation, applying a given projection.

        :param id_list: The list of IDs to query by
        :param required_fields: The fields that must be included in the audio document
        :param optional_fields: The fields to include if present
        :param excluded_fields: The fields to omit
        :return: The audio document with the best evaluation with the given projection applied
        """
        query = {"_id": {"$in": id_list}, "version": self.config["VERSION"]}
        if self.audio.count_documents(query) == 0:
            self.logger.error("No audio documents found")
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
            self._debug_variable("audio_doc", audio_doc)
            if all(field in audio_doc for field in required_fields):
                return audio_doc
            self.logger.warning(f"Audio document is missing at least one required field: {', '.join(required_fields)}. Skipping")

        self.logger.error("Failed to find a viable audio document")

    def find_questions(self, qids: list = None, **kwargs):
        """
        Generator function for getting questions from both collections.

        :param qids: The list of question IDs to search through
        :param kwargs: Pass in any additional arguments for the find() function.
        :return: A generator that can be iterated through to get each result from UnrecordedQuestions and
                 RecordedQuestions.
        """
        kwargs_c = deepcopy(kwargs)

        # Overrides _id argument in filter.
        if qids:
            if "filter" not in kwargs_c:
                kwargs_c["filter"] = {}
            kwargs_c["filter"]["qb_id"] = {"$in": qids}

        self.logger.info("Finding unrecorded questions...")
        unrec_cursor = self.unrec_questions.find(**kwargs_c)
        found_unrec_qids = []
        for i, question in enumerate(unrec_cursor):
            self._debug_variable(f"question {i}", question)
            found_unrec_qids.append(question["qb_id"])
            yield question
        self.logger.info(f"Found {len(found_unrec_qids)} unrecorded question(s)")

        if qids:
            rec_qids = [qid for qid in qids if qid not in found_unrec_qids]
            if not rec_qids:
                self.logger.info("Found all questions. Skipping finding recorded questions")
                return
            kwargs_c["filter"]["qb_id"] = {"$in": rec_qids}
            self.logger.info(f"Finding {len(rec_qids)} of {len(qids)} recorded question(s)...")
        else:
            self.logger.info("Finding recorded questions...")

        rec_count = self.rec_questions.count_documents(**kwargs_c)
        rec_cursor = self.rec_questions.find(**kwargs_c)
        for i, question in enumerate(rec_cursor):
            self._debug_variable(f"question {i}", question)
            yield question
        self.logger.info(f"Found {rec_count} recorded question(s)")

    def pick_random_question(self,
                             question_ids: List[int],
                             required_fields: List[str]):
        """
        Pick a random question from a list of question IDs and find it. Alias for ``pick_random_questions`` without a
        ``batch_size`` argument.

        :param question_ids: The list of question IDs to select from
        :param required_fields: Require these fields to be present in the returned document.
        :return: A randomly selected question
        """
        return self.pick_random_questions(question_ids, required_fields)

    def pick_random_questions(self, question_ids: List[int],
                              required_fields: List[str],
                              batch_size: int = 1):
        """
        Search for multiple questions from a shuffled list of question IDs.

        :param question_ids: The list of question IDs to select from
        :param required_fields: Require these fields to be present in the returned documents.
        :param batch_size: The number of questions to retrieve
        :return: A list of randomly selected questions
        """
        qids_pool = question_ids.copy()
        random.shuffle(qids_pool)
        next_batch_size = batch_size
        sentences = []
        errors = []
        while qids_pool:
            if len(qids_pool) >= next_batch_size:
                next_id_batch = qids_pool[:next_batch_size]
                qids_pool = qids_pool[next_batch_size:]
            else:
                next_id_batch = qids_pool.copy()
                qids_pool = []
            self._debug_variable("next_id_batch", next_id_batch)
            self._debug_variable("qids_pool", qids_pool)
            questions_cursor = chain(
                self.unrec_questions.find({"qb_id": {"$in": next_id_batch}}),
                self.rec_questions.find({"qb_id": {"$in": next_id_batch}})
            )
            found = set()
            for doc in questions_cursor:
                self._debug_variable("doc", doc)
                valid_doc = True
                for field in required_fields:
                    if field not in doc:
                        valid_doc = False
                        self.logger.warning(f"Question does not contain required field '{field}'. Ignoring")
                        errors.append({"questionId": repr(doc["_id"]), "reason": f"missing_{field}"})
                        break
                if valid_doc:
                    sentences.append(doc)
                    found.add(doc["qb_id"])
            next_batch_size -= len(found)
            if next_batch_size == 0:
                self.logger.info("Found all questions requested. Returning results")
                return sentences, errors
            self.logger.info(f"Found {len(found)} of {batch_size} questions requested. Searching for {next_batch_size} more...")
        if not qids_pool:
            self.logger.info("Could not find any more valid questions. Returning results")
        if sentences:
            return sentences, errors
        self.logger.error("Failed to find any viable questions. Aborting")
        return None, errors

    # Utility methods for automatically updating the cached ID list.
    def insert_unrec_question(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.insert_one(*args, **kwargs)
        self.unrec_question_ids.append(results.inserted_id)
        return results

    def insert_unrec_questions(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.insert_many(*args, **kwargs)
        self.unrec_question_ids += results.inserted_ids
        return results

    def delete_unrec_question(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.delete_one(*args, **kwargs)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        return results

    def delete_unrec_questions(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.unrec_questions.delete_many(*args, **kwargs)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        return results

    def insert_rec_question(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.insert_one(*args, **kwargs)
        self.rec_question_ids.append(results.inserted_id)
        return results

    def insert_rec_questions(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.insert_many(*args, **kwargs)
        self.rec_question_ids += results.inserted_ids
        return results

    def delete_rec_question(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.delete_one(*args, **kwargs)
        self.rec_question_ids = self.get_ids(self.rec_questions)
        return results

    def delete_rec_questions(self, *args, **kwargs):
        """**DEPRECATED!** Wrapper method that updates the cached ID list to reflect the results of the operation."""
        results = self.rec_questions.delete_many(*args, **kwargs)
        self.rec_question_ids = self.get_ids(self.rec_questions)
        return results

    def upload_many(self, file_paths: List[str], subdir: str) -> Dict[str, str]:
        """
        Upload multiple audio files to Firebase Cloud Storage, located at ``<BLOB_ROOT>/<subdir>/``.

        :param file_paths: The paths of the files to upload
        :param subdir: The subdirectory to put the files in
        :return: A dictionary mapping file names to blob names
        """
        # TODO: Actual BrokenPipeError handling
        file2blob = {}

        self.logger.info(f"Uploading {len(file_paths)} file(s)...")
        upload_count = 0
        for file_path in file_paths:
            file_name = os.path.basename(file_path)
            # blob_name = token_urlsafe(self.config["BLOB_NAME_LENGTH"])
            blob_name = str(uuid4())
            blob_path = self.get_blob_path(blob_name, subdir)
            blob = self.bucket.blob(blob_path)
            blob.upload_from_filename(file_path)
            file2blob[file_name] = blob_name
            self.logger.debug(f"{upload_count}/{len(file_paths)}")
            upload_count += 1

        return file2blob

    def get_blob_path(self, blob_name: str, subdir: str) -> str:
        """
        Form a path from a base name in the format ``<BLOB_ROOT>/<subdir>/<blob_name>``.

        :param blob_name: The base name of the blob
        :param subdir: The directory/ies to put the file in
        :return: A blob "path"
        """
        return "/".join([self.config["BLOB_ROOT"], subdir, blob_name])

    def upload_one(self, file_path: str, subdir: str) -> str:
        """
        Upload one audio file to Firebase Cloud Storage.

        :param file_path: The path of the file to upload
        :param subdir: The subdirectory to put the file in
        :return: The name of the resulting blob
        """
        # TODO: Actual BrokenPipeError handling
        file_name = os.path.basename(file_path)
        return self.upload_many([file_path], subdir)[file_name]

    def mongodb_insert_submissions(
            self,
            sub2blob: Dict[str, str],
            sub2meta: Dict[str, Dict[str, Any]],
            sub2vtt: Dict[str, str],
            sub2score: Dict[str, Dict[str, float]]
    ) -> Tuple[InsertManyResult, InsertManyResult]:
        """
        Upload submission metadata to MongoDB.

        :param sub2blob: A dictionary mapping submissions to blob names
        :param sub2meta: A dictionary mapping submissions to metadata to use. Fields that start with '__' are not
                         included.
        :param sub2vtt: A dictionary mapping submissions to VTTs.
        :param sub2score: A dictionary mapping submissions to ASR scores.
        :return: A tuple containing the results from inserting to the Audio and UnprocessedAudio collections
                 respectively.
        """
        processing_list = ["normal"]
        question_audio_batch = []
        other_audio_batch = []
        uid2rec_docs = {}
        qid2rec_docs = {}
        self.logger.info("Preparing document entries...")
        for submission, audio_id in sub2blob.items():
            entry = {
                "_id": audio_id,
                "version": self.config["VERSION"]
            }
            metadata = sub2meta[submission]
            for k, v in metadata.items():
                if not k.startswith("__"):
                    entry[k] = v
            if metadata["recType"] in processing_list:
                entry["vtt"] = sub2vtt[submission]
                entry["score"] = sub2score[submission]
                # TODO: Support for questions segmented into multiple documents
                if metadata["qb_id"] not in qid2rec_docs:
                    qid2rec_docs[metadata["qb_id"]] = []
                qid2rec_docs[metadata["qb_id"]].append({"id": audio_id, "recType": metadata["recType"]})
            if metadata["recType"] not in processing_list:
                other_audio_batch.append(entry)
            else:
                question_audio_batch.append(entry)

            if metadata["userId"] not in uid2rec_docs:
                uid2rec_docs[metadata["userId"]] = []
            uid2rec_docs[metadata["userId"]].append({"id": audio_id, "recType": metadata["recType"]})

            self._debug_variable("entry", entry)

        self._debug_variable("question_audio_batch", question_audio_batch)
        self._debug_variable("other_audio_batch", other_audio_batch)

        if not question_audio_batch:
            self.logger.info("No question recordings to insert into the Audio collection. Skipping")
            question_rec_results = None
        else:
            question_rec_results = self.audio.insert_many(question_audio_batch)
            self.add_recs_to_questions(qid2rec_docs)
            self.logger.info(f"Inserted {len(question_rec_results.inserted_ids)} question recording(s) into the Audio collection")

        if not other_audio_batch:
            self.logger.info("No buzz or answer recordings to insert into the Audio collection. Skipping")
            proc_results = None
        else:
            proc_results = self.audio.insert_many(other_audio_batch)
            self.logger.info(f"Inserted {len(proc_results.inserted_ids)} buzz and/or answer recording(s) into the Audio collection")
        self.add_recs_to_users(uid2rec_docs)
        return question_rec_results, proc_results

    def get_profile(self, user_id: str, visibility: str) -> Optional[dict]:
        """
        Retrieve a User document from the associated MongoDB collection.

        :param user_id: The internal ID of the User document
        :param visibility: How much of the profile to show. Valid values are "basic", "public", and "private"
        :return: A document from the MongoDB Users collection
        """
        visibility_configs = self.config["VISIBILITY_CONFIGS"]
        config = visibility_configs[visibility]
        return self.database.get_collection(config["collection"]).find_one(user_id, config["projection"])

    def create_profile(self, user_id: str, pfp: List[str], username: str):
        """
        Create a profile stub from the given parameters.

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :param pfp: Freeform array for the profile. Potential values can be for color, the types of images to use, etc.
        :param username: The public name of the user. Must not conflict with any existing usernames
        :return: A pymongo InsertOneResult object. See documentation for further details
        :raise UserExistsError: When there is an existing user profile with the given username
        :raise pymongo.errors.DuplicateKeyError: When a user of the given ID already exists
        """
        if self.users.find_one({"username": username}) is not None:
            raise UsernameTakenError(username)
        profile = {
            "_id": user_id,
            "pfp": pfp,
            "username": username,
            "usernameSpecs": '',
            "ratings": {},
            "totalQuestionsPlayed": 0,
            "totalGames": 0,
            "coins": 0,
            "coinsCumulative": 0,
            "activityOverview": [],
            "recordedAudios": [],
            "permLevel": "normal",
            "playTime": 0,
            "creationDate": datetime.now().isoformat(),
            "recVotes": []
        }
        return self.users.insert_one(profile)

    def modify_profile(self, user_id: str, update_args: Dict[str, Any]):
        """
        Modify a user profile.

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :param update_args: The fields to replace and their corresponding values
        :return: A pymongo UpdateResult object. See documentation for further details
        """
        username = update_args.get("username")
        if username and self.users.find_one({"username": username}) is not None:
            raise UsernameTakenError(username)
        return self.users.update_one({"_id": user_id}, {"$set": update_args})

    def delete_profile(self, user_id: str):
        """
        Delete a user profile.

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :return: A pymongo DeleteResult object. See documentation for further details
        """
        return self.users.delete_one({"_id": user_id})

    def get_user_role(self, user_id: str) -> str:
        """
        Get the permission level of the user associated with the given ID.

        :param user_id: The internal ID of a user, defined by the _id field of a profile document
        :return: The role of the user
        :raise ProfileNotFoundError: When the profile associated with the given ID does not exist
        :raise MalformedProfileError: When the profile associated with the given ID is missing the permission level
        """
        profile = self.users.find_one({"_id": user_id}, {"permLevel": 1})
        if not profile:
            raise ProfileNotFoundError(f"'{user_id}'")
        if "permLevel" not in profile:
            raise MalformedProfileError(f"Field 'permLevel' not found in profile for user '{user_id}'")
        return profile["permLevel"]

    def increment_num_recs(self, user_id: str, difficulty: int):
        """
        Increment the ``"numRecs"`` stat for a user.

        :param user_id: The ID of the user profile
        :param difficulty: The recording difficulty type to increment, including "all"
        """
        self.users.update_one({"_id": user_id}, {
            "$inc": {
                "stats.recordings.numRecs.all": 1,
                f"stats.recordings.numRecs.{difficulty}": 1
            }
        })

    def add_rec_rating(self, audio_id: str, user_id: str, rating: float):
        """
        Push a rating for a given audio recording by a given user and call the ``set_avg_rec_rating`` method.

        :param audio_id: The ID of the audio document to be rated
        :param user_id: The ID of the user who is rating the audio document
        :param rating: The rating the user is leaving
        """
        audio_doc = self.audio.find_one_and_update({"_id": audio_id}, {
            "$set": {
                f"ratings.{user_id}": rating
            }
        }, return_document=pymongo.ReturnDocument.AFTER)
        total_rating = 0
        for rating_ in audio_doc["ratings"].values():
            total_rating += rating_
        avg_rating = total_rating / len(audio_doc["ratings"])
        self.set_avg_rec_rating(audio_id, avg_rating)

    def set_avg_rec_rating(self, audio_id: str, rating: float):
        """
        Set the average recording rating of an audio recording and update the stats of the associated user.

        :param audio_id: The ID of the recording to set
        :param rating: The average rating to set
        :raise ProfileNotFoundError: If the user of the audio recording does not exist
        :raise MalformedProfileError: If the user of the audio recording does not have it embedded in their profile
        """
        audio_doc = self.audio.find_one_and_update({"_id": audio_id}, {
            "$set": {
                "avgRating": rating
            }
        })
        self._debug_variable("audio_doc", audio_doc)
        user_id = audio_doc["userId"]
        profile = self.users.find_one({"_id": user_id})
        self._debug_variable("profile", profile)
        if not profile:
            raise ProfileNotFoundError(f"'{user_id}'")
        rec_index = QuizzrTPM.index_of_rec(profile["recordedAudios"], audio_id)
        self._debug_variable("rec_index", rec_index)
        if rec_index is None:
            raise MalformedProfileError(f"Expected recording with ID '{audio_id}' in user profile with ID '{user_id}'")

        question = self.rec_questions.find_one({"qb_id": audio_doc["qb_id"]})
        self._debug_variable("question", question)
        difficulty = self.get_difficulty_type(question["recDifficulty"])
        self._debug_variable("difficulty", difficulty)
        total_ratings_all = rating
        total_ratings_diff = rating
        num_rated_recs_all = 1
        num_rated_recs_diff = 1
        for i, rec_doc in enumerate(profile["recordedAudios"]):
            if "avgRating" not in rec_doc or i == rec_index:
                continue
            total_ratings_all += rec_doc["avgRating"]
            num_rated_recs_all += 1
            # TODO: Use embedded recording difficulty type
            rec_doc_lookup = self.audio.find_one({"_id": rec_doc["id"]}, {"qb_id": 1})
            self._debug_variable("rec_doc_lookup", rec_doc_lookup)
            rec_doc_question = self.rec_questions.find_one({"qb_id": rec_doc_lookup["qb_id"]})
            self._debug_variable("rec_doc_question", rec_doc_question)
            rec_doc_difficulty = self.get_difficulty_type(rec_doc_question["recDifficulty"])
            self._debug_variable("rec_doc_difficulty", rec_doc_difficulty)
            if rec_doc_difficulty == difficulty:
                total_ratings_diff += rec_doc["avgRating"]
                num_rated_recs_diff += 1

        avg_rating_all = total_ratings_all / num_rated_recs_all
        avg_rating_diff = total_ratings_diff / num_rated_recs_diff
        self.users.update_one({"_id": user_id}, {
            "$set": {
                f"recordedAudios.{rec_index}.avgRating": rating,
                "stats.recordings.avgRating.all": avg_rating_all,
                f"stats.recordings.avgRating.{difficulty}": avg_rating_diff
            }
        })

    def get_difficulty_type(self, rec_difficulty: float) -> Optional[int]:
        """
        Get the difficulty type from the recording difficulty.

        :param rec_difficulty: The recDifficulty field from a question
        :return: The difficulty type, or None if not applicable
        """
        difficulty_limits = self.config["DIFFICULTY_LIMITS"]
        for difficulty, upper_bound in enumerate(difficulty_limits):
            lower_bound = difficulty_limits[difficulty - 1] if difficulty > 0 else None
            if (not lower_bound or lower_bound <= rec_difficulty) and (not upper_bound or rec_difficulty < upper_bound):
                return difficulty

    def _debug_variable(self, name: str, v, include_type=False):
        """
        Send a variable's name and value to the given logger.

        :param name: The name of the variable to display in the logger
        :param v: The value of the variable
        :param include_type: Whether to include the type() of the variable
        """
        if include_type:
            prefix = f"{type(v)} "
        else:
            prefix = ""
        val_pp = pprint.pformat(v)
        self.logger.debug(f"{prefix}{name} = {val_pp}")

    @staticmethod
    def get_difficulty_query_op(difficulty_limits: list, difficulty: int) -> dict:
        """
        Given the list of difficulty limits and a difficulty type, return the boundaries as a MongoDB filter
        operator.

        :param difficulty_limits: The upper bound for each difficulty level
        :param difficulty: The difficulty level to choose
        :return: The boundaries as a MongoDB filter operator
        """
        lower_bound = difficulty_limits[difficulty - 1] if difficulty > 0 else None
        upper_bound = difficulty_limits[difficulty]
        query_op = {}
        if lower_bound:
            query_op["$gte"] = lower_bound
        if upper_bound:
            query_op["$lt"] = upper_bound
        return query_op

    @staticmethod
    def get_ids(collection: pymongo.collection.Collection, query: dict = None) -> list:
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
    def index_of_rec(recordings: List[dict], target_id: str) -> Optional[int]:
        """
        Get the index of an embedded recording document in a profile.

        :param recordings: The list of embedded recording documents to search through
        :param target_id: The target audio ID
        :return: The index of the recording in the list, or None if not found
        """
        for i, rec_doc in enumerate(recordings):
            if rec_doc["id"] == target_id:
                return i
