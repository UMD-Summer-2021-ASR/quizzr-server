import atexit
import logging
import os
import re
import signal

import gentle
import sys
import time
from typing import List, Dict, Union

import bson.json_util

import forced_alignment
import vtt_conversion


class QuizzrWatcher:
    def __init__(self, watch_dir, func, interval=2, queue_size_limit=32):
        self.done = False
        self.watch_dir = watch_dir
        self.queue_size_limit = queue_size_limit
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
            queued_submissions = self.queue_submissions(self.watch_dir, self.queue_size_limit)
            if queued_submissions:
                self.func(queued_submissions)
            time.sleep(self.interval)

        sys.exit(0)

    # Return a list of submission names. Submissions have a WAV file and an associated JSON metadata file.
    @staticmethod
    def queue_submissions(directory: str, size_limit: int = None):
        # TODO: Add queue size limit.
        found_files = os.listdir(directory)
        queued_submissions = set()
        for i, found_file in enumerate(found_files):
            if size_limit and i > size_limit:
                break
            submission_name = os.path.splitext(found_file)[0]
            queued_submissions.add(submission_name)
        return queued_submissions


class QuizzrProcessor:
    def __init__(self, database, directory: str, version: str):
        self.VERSION = version
        self.DIRECTORY = directory
        self.SUBMISSION_FILE_TYPES = ["wav", "json", "vtt"]
        # self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES") or 5)

        self.users = database.Users
        self.rec_questions = database.RecordedQuestions
        self.unrec_questions = database.UnrecordedQuestions
        self.audio = database.Audio
        self.unproc_audio = database.UnprocessedAudio

        self.punc_regex = re.compile(r"[.?!,;:\"\-]")
        self.whitespace_regex = re.compile(r"\s+")
        self.ACCURACY_CUTOFF = 0.5  # Hyperparameter
        self.ERROR_ACCURACY = -1.0

    # Pre-screen all given normal submissions and return the ones that passed the pre-screening.
    # Return buzz recordings immediately.
    def pick_submissions(self, submissions: List[str]) -> dict:
        logging.info(f"Gathering metadata for {len(submissions)} submission(s)...")
        sub2meta = self.get_metadata(submissions)
        logging.debug(f"sub2meta = {sub2meta!r}")
        typed_submissions = QuizzrProcessor.categorize_submissions(submissions, sub2meta)

        final_results = {}
        num_accepted_submissions = 0
        if "normal" in typed_submissions:
            results = self.preprocess_submissions(typed_submissions['normal'], sub2meta)
            for submission in typed_submissions['normal']:
                if "err" in results[submission]:
                    final_results[submission] = {"case": "err", "err": results[submission]["err"]}
                elif results[submission]["accuracy"] < self.ACCURACY_CUTOFF:
                    final_results[submission] = {"case": "rejected"}
                    delete_submission(self.DIRECTORY, submission, self.SUBMISSION_FILE_TYPES)
                else:
                    final_results[submission] = {
                        "case": "accepted",
                        "vtt": results[submission]["vtt"],
                        "metadata": sub2meta[submission]
                    }
                    num_accepted_submissions += 1
            logging.info(f"Accepted {num_accepted_submissions} of {len(typed_submissions['normal'])} submission(s)")

        if "buzz" in typed_submissions:
            for submission in typed_submissions["buzz"]:
                final_results[submission] = {
                    "case": "accepted",
                    "metadata": sub2meta[submission]
                }
            logging.info(f"Received {len(typed_submissions['buzz'])} submission(s) for buzz-ins.")
        return final_results

    # Return the accuracy and VTT data of each submission.
    def preprocess_submissions(self,
                               submissions: List[str],
                               sub2meta: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, Union[float, str]]]:
        # TODO: Make into batches if possible.
        logging.info(f"Evaluating {len(submissions)} submission(s)...")

        num_finished_submissions = 0
        results = {}
        for submission in submissions:
            results[submission] = {}
            file_path = os.path.join(self.DIRECTORY, submission)
            wav_file_path = file_path + ".wav"
            # json_file_path = file_path + ".json"
            if sub2meta.get(submission) is None:
                logging.error(f"Metadata for submission {submission} not found. Skipping")
                results[submission]["err"] = "meta_not_found"
                continue
            qid = sub2meta[submission].get("questionId")
            if qid is None:
                logging.error(f"Question ID for submission {submission} not found. Skipping")
                results[submission]["err"] = "qid_not_found"
                continue

            logging.debug(f"{type(qid)} qid = {qid!r}")
            logging.debug("Finding question in UnrecordedQuestions...")
            question = self.unrec_questions.find_one({"_id": qid}, {"transcript": 1})
            if question is None:
                logging.debug("Question not found in UnrecordedQuestions. Searching in RecordedQuestions...")
                question = self.rec_questions.find_one({"_id": qid}, {"transcript": 1})

            if question is None:
                logging.error("Question not found. Skipping submission")
                results[submission]["err"] = "question_not_found"
                continue

            r_transcript = question.get("transcript")

            if r_transcript is None:
                logging.error("Transcript not found. Skipping submission")
                results[submission]["err"] = "transcript_not_found"
                continue

            accuracy, vtt = self.get_accuracy_and_vtt(wav_file_path, r_transcript)
            if accuracy is None:
                results[submission]["err"] = "runtime_error"
                continue
            # accuracy = self.ACCURACY_CUTOFF
            # accuracy = random.random()

            num_finished_submissions += 1

            logging.info(f"Evaluated {num_finished_submissions}/{len(submissions)} submissions")
            logging.debug(f"Alignment for '{submission}' has accuracy {accuracy}")
            results[submission]["accuracy"] = accuracy
            results[submission]["vtt"] = vtt
        return results

    # ****************** HELPER METHODS *********************
    # Do a forced alignment and return the percentage of words aligned along with the VTT.
    def get_accuracy_and_vtt(self, file_path: str, r_transcript: str):
        total_words = len(self.process_transcript(r_transcript))
        total_aligned_words = 0
        try:
            alignment = forced_alignment.get_forced_alignment(file_path, r_transcript)
        except RuntimeError as e:
            logging.error(f"Encountered RuntimeError: {e}. Aborting")
            return None, None
        words = alignment.words
        for word_data in words:
            if word_data.case == "success":
                total_aligned_words += 1
        vtt = vtt_conversion.gentle_alignment_to_vtt(words)

        return total_aligned_words / total_words, vtt

    # Return a list of words without punctuation or capitalization from a transcript.
    def process_transcript(self, t: str) -> List[str]:
        return re.split(self.whitespace_regex, re.sub(self.punc_regex, " ", t).lower())

    # Return the metadata of each submission (a JSON file) if available.
    def get_metadata(self, submissions: List[str]) -> Dict[str, Dict[str, str]]:
        sub2meta = {}
        for submission in submissions:
            submission_path = os.path.join(self.DIRECTORY, submission)
            if not os.path.exists(submission_path + ".json"):
                continue
            with open(submission_path + ".json", "r") as meta_f:
                sub2meta[submission] = bson.json_util.loads(meta_f.read())
        return sub2meta

    @staticmethod
    def categorize_submissions(submissions, sub2meta):
        typed_submissions = {}
        for submission in submissions:
            submission_type = sub2meta[submission]["recType"]
            if submission_type not in typed_submissions:
                typed_submissions[submission_type] = []
            typed_submissions[submission_type].append(submission)
        return typed_submissions

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


# Remove a submission from disk.
def delete_submission(directory, submission_name, file_types):
    # TODO: Make it find all files with submission_name instead.
    submission_path = os.path.join(directory, submission_name)
    for ext in file_types:
        file_path = ".".join([submission_path, ext])
        if os.path.exists(file_path):
            os.remove(file_path)
