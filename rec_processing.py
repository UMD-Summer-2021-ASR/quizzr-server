import atexit
import logging
import os
import random
import re
import signal
import sys
import time
from datetime import datetime
from typing import List, Dict, Any

import bson.json_util
import pymongo
from bson import ObjectId
from googleapiclient.http import MediaFileUpload

import forced_alignment
import gdrive_authentication

logging.basicConfig(level=logging.DEBUG)


class QuizzrWatcher:
    def __init__(self, watch_dir, func, interval=2):
        self.done = False
        self.watch_dir = watch_dir
        self.func = func
        self.interval = interval
        atexit.register(self.exit_handler)
        signal.signal(signal.SIGINT, self.signal_handler)

    def exit_handler(self):
        print('Exit handler executed!')

    def signal_handler(self, signal, frame):
        self.done = True

    def execute(self):
        while not self.done:
            queued_submissions = self.queue_submissions(self.watch_dir)
            if queued_submissions:
                self.func(queued_submissions)
            time.sleep(self.interval)

        sys.exit(0)

    @staticmethod
    def queue_submissions(directory: str):
        found_files = os.listdir(directory)
        queued_submissions = set()
        for found_file in found_files:
            submission_name = found_file.split(".")[0]
            queued_submissions.add(submission_name)
        return queued_submissions


class QuizzrProcessor:
    def __init__(self, database, directory: str, version: str, secret_directory: str, gdrive):
        self.VERSION = version
        self.DIRECTORY = directory

        self.users = database.Users
        self.rec_questions = database.RecordedQuestions
        self.unrec_questions = database.UnrecordedQuestions
        self.audio = database.Audio
        self.unproc_audio = database.UnprocessedAudio

        self.gdrive = gdrive

        self.punc_regex = re.compile(r"[.?!,;:\"\-]")
        self.whitespace_regex = re.compile(r"\s+")
        self.RECOGNIZER_TYPE = "Gentle Forced Aligner"  # Easier to refactor.
        self.ACCURACY_CUTOFF = 0.5  # Hyperparameter

    # submissions do not have extensions
    def pick_submissions(self, submissions: List[str]):
        logging.log(logging.INFO, f"Gathering metadata for {len(submissions)} submission(s)...")
        sub2meta = self.get_metadata(submissions)

        accepted_submissions = []
        accuracies = self.preprocess_submissions(submissions, sub2meta)
        for submission in submissions:
            if accuracies[submission] < self.ACCURACY_CUTOFF:
                self.delete_submission(submission)
            else:
                accepted_submissions.append(submission)

        logging.log(
            logging.INFO,
            f"Accepted {len(accepted_submissions)} of {len(submissions)} submission(s)"
        )

        subs2gfids = self.gdrive_upload_many(accepted_submissions)
        self.mongodb_insert_submissions(subs2gfids, sub2meta)
        for submission in accepted_submissions:
            self.delete_submission(submission)
        return accepted_submissions

    def preprocess_submissions(self, submissions: List[str], sub2meta: Dict[str, Dict[str, str]]) -> Dict[str, float]:
        # TODO: Make into batches if possible.
        logging.log(logging.INFO, f"Evaluating {len(submissions)} submission(s)...")

        num_finished_submissions = 0
        results = {}
        for submission in submissions:
            file_path = os.path.join(self.DIRECTORY, submission)
            wav_file_path = file_path + ".wav"
            # json_file_path = file_path + ".json"
            question = self.unrec_questions.find_one({"_id": sub2meta[submission]["questionId"]}, {"transcript": 1})
            if question is None:
                question = self.rec_questions.find_one({"_id": sub2meta[submission]["questionId"]}, {"transcript": 1})

            r_transcript = question["transcript"]

            accuracy = self.get_accuracy(wav_file_path, r_transcript)
            # accuracy = self.ACCURACY_CUTOFF
            # accuracy = random.random()

            num_finished_submissions += 1

            logging.log(logging.INFO, f"Evaluated {num_finished_submissions}/{len(submissions)} submissions")
            logging.log(logging.DEBUG, f"Alignment for '{submission}' has accuracy {accuracy}")
            results[submission] = accuracy
        return results

    def gdrive_upload_many(self, submissions: List[str]) -> Dict[str, str]:
        subs2gfids = {}
        if self.gdrive.creds.expired:
            self.gdrive.refresh()

        # TODO: Convert to batch request
        for submission in submissions:
            file_path = os.path.join(self.DIRECTORY, submission)
            wav_file_path = file_path + ".wav"

            file_metadata = {"name": str(datetime.now()) + ".wav"}
            media = MediaFileUpload(wav_file_path, mimetype="audio/wav")
            gfile = self.gdrive.drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
            gfile_id = gfile.get("id")

            permission = {"type": "anyone", "role": "reader"}
            self.gdrive.drive.permissions().create(fileId=gfile_id, body=permission, fields="id").execute()
            subs2gfids[submission] = gfile_id
        return subs2gfids

    def mongodb_insert_submissions(self, subs2gfids: Dict[str, str], sub2meta: Dict[str, Dict[str, Any]]):
        mongodb_insert_batch = []
        for submission in subs2gfids.keys():
            entry = {"_id": subs2gfids[submission], "version": self.VERSION}
            entry.update(sub2meta[submission])
            mongodb_insert_batch.append(entry)
        results = self.unproc_audio.insert_many(mongodb_insert_batch)
        logging.log(logging.INFO, f"Inserted {len(results.inserted_ids)} documents into the UnprocessedAudio collection")
        return results

    # Helper methods
    def delete_submission(self, submission_name):
        # TODO: Make it find all files with submission_name instead.
        submission_path = os.path.join(self.DIRECTORY, submission_name)
        os.remove(submission_path + ".wav")
        os.remove(submission_path + ".json")

    def get_accuracy(self, file_path: str, r_transcript: str):
        total_words = len(self.process_transcript(r_transcript))
        total_aligned_words = 0
        alignment = forced_alignment.get_forced_alignment(file_path, r_transcript)
        # alignment = {}
        words = alignment["words"]
        for word_data in words:
            if word_data["case"] == "success":
                total_aligned_words += 1

        return total_aligned_words / total_words

    def process_transcript(self, t: str) -> List[str]:
        return re.split(self.whitespace_regex, re.sub(self.punc_regex, " ", t).lower())

    def get_metadata(self, submissions: List[str]) -> Dict[str, Dict[str, str]]:
        sub2meta = {}
        for submission in submissions:
            file_path = os.path.join(self.DIRECTORY, submission)
            with open(file_path + ".json", "r") as meta_f:
                sub2meta[submission] = bson.json_util.loads(meta_f.read())
        return sub2meta

    # Kept here for reference.
    """def process_submissions(self, directory: str, submissions: List[str]):
        # luigi.run(directory, output_directory)
        new_audio_descriptors = []
        q2recs = {}
        u2recs = {}

        for submission in submissions:
            audio_entry = {}

            wav_file_path = os.path.join(directory, submission)
            json_file_path = wav_file_path.split(".wav")[0] + ".json"
            vtt_file_path = wav_file_path.split(".wav")[0] + ".vtt"
            transcript_path = wav_file_path.split(".wav")[0] + ".txt"
            original_transcript = ""

            file_id = self.upload_to_gdrive(wav_file_path)
            with open(json_file_path) as meta_f:
                metadata = json.load(meta_f)
                question_id = metadata["question_id"]
                user_id = metadata["userId"]
            with open(vtt_file_path) as vtt_f:
                vtt_contents = vtt_f.read()

            audio_entry["_id"] = file_id
            audio_entry["questionId"] = question_id
            audio_entry["userId"] = user_id
            audio_entry["vtt"] = vtt_contents
            audio_entry["accuracy"] = self.get_accuracy(transcript_path, original_transcript)
            audio_entry["version"] = self.VERSION
            new_audio_descriptors.append(audio_entry)

            self.push_recording(q2recs, file_id, question_id)
            self.push_recording(u2recs, file_id, user_id)

            os.remove(wav_file_path)
            os.remove(json_file_path)
            os.remove(vtt_file_path)
        self.audio.insert_many(new_audio_descriptors)

        new_question_ids = q2recs.keys()
        unrec_cursor = self.unrec_questions.find({"_id": {"$in": new_question_ids}})
        found_unrec_questions = [ question for question in unrec_cursor ]
        found_unrec_qids = [ question["_id"] for question in found_unrec_questions ]
        found_rec_qids = [ qid for qid in new_question_ids if qid not in found_unrec_qids ]

        q_batch = []
        u_batch = []

        for audio_entry in new_audio_descriptors:
            if audio_entry["questionId"] in found_rec_qids:
                query_qid = {"_id": audio_entry["questionId"]}
                q_op = {"$push": {"recordings": audio_entry["_id"]}}
                q_batch.append(pymongo.UpdateOne(query_qid, q_op))
            else:
                question = None
            query_uid = {"_id": audio_entry["userId"]}
            u_op = {"$push": {"recordedAudios": audio_entry["_id"]}}
            u_batch.append(pymongo.UpdateOne(query_uid, u_op))

        self.unrec_questions.delete_many()
        self.rec_questions.insert_many(found_unrec_questions)
        self.rec_questions.bulk_write(q_batch)
        self.users.bulk_write(u_batch)"""


if __name__ == "__main__":
    SERVER_DIR = os.path.dirname(__file__)
    SECRET_DATA_DIR = os.path.join(SERVER_DIR, "privatedata")
    REC_DIR = os.path.join(SERVER_DIR, "recordings")
    with open(os.path.join(SECRET_DATA_DIR, "connectionstring")) as f:
        mongodb_client = pymongo.MongoClient(f.read())
    gdrive = gdrive_authentication.GDriveAuth(SECRET_DATA_DIR)
    processor = QuizzrProcessor(mongodb_client.QuizzrDatabase, REC_DIR, "D_0.0.0", SECRET_DATA_DIR, gdrive)
    # processor = QuizzrProcessor(None, REC_DIR, "D_0.0.0", SECRET_DATA_DIR)
    watcher = QuizzrWatcher(REC_DIR, processor.pick_submissions)
    watcher.execute()
