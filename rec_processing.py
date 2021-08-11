import atexit
import logging
import os
import re
import signal

import sys
import time
from typing import List, Dict, Union

import bson.json_util
from pymongo.database import Database

import forced_alignment
import vtt_conversion


class QuizzrWatcher:
    def __init__(self, watch_dir: str, func, interval: float = 2, queue_size_limit: int = 32):
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

    @staticmethod
    def queue_submissions(directory: str, size_limit: int = None):
        """
        Get up to <size_limit> submission names

        :param directory: The directory to get the submission names from
        :param size_limit: The maximum number of submission names allowable
        :return: A set of submission names
        """
        found_files = os.listdir(directory)
        queued_submissions = set()
        for i, found_file in enumerate(found_files):
            if size_limit and i > size_limit:
                break
            submission_name = os.path.splitext(found_file)[0]
            queued_submissions.add(submission_name)
        return queued_submissions


class QuizzrProcessor:
    def __init__(self, database: Database, directory: str, config: dict, submission_file_types: List[str]):
        self.logger = logging.getLogger(__name__)
        self.submission_file_types = submission_file_types
        self.config = config
        self.DIRECTORY = directory
        # self.MAX_RETRIES = int(os.environ.get("MAX_RETRIES") or 5)

        self.users = database.Users
        self.rec_questions = database.RecordedQuestions
        self.unrec_questions = database.UnrecordedQuestions
        self.audio = database.Audio
        self.unproc_audio = database.UnprocessedAudio

        self.punc_regex = re.compile(r"[.?!,;:\"\-]")
        self.whitespace_regex = re.compile(r"\s+")
        self.ERROR_ACCURACY = -1.0

    def pick_submissions(self, submissions: List[str]) -> dict:
        """
        Pre-screen all given normal submissions and return the ones that passed the pre-screening.
        Return buzz recordings immediately.

        :param submissions: A list of submission names. A submission inside a directory must have a WAV file and a JSON
                            file associated with it.
        :return: A dictionary containing the results of each submission, which always contains a "case" key. If the case
                 is "accepted", it will contain a "metadata" key and may contain a "vtt" and "accuracy" key if it is a
                 normal recording. If the case is "err", it will include a machine-readable error message.
        """
        self.logger.info(f"Gathering metadata for {len(submissions)} submission(s)...")
        sub2meta = self.get_metadata(submissions)
        self.logger.debug(f"sub2meta = {sub2meta!r}")
        typed_submissions = QuizzrProcessor.categorize_submissions(submissions, sub2meta)

        final_results = {}
        num_accepted_submissions = 0
        if "normal" in typed_submissions:
            results = self.preprocess_submissions(typed_submissions['normal'], sub2meta)
            for submission in typed_submissions['normal']:
                if "err" in results[submission]:
                    final_results[submission] = {"case": "err", "err": results[submission]["err"]}
                elif results[submission]["accuracy"] < self.config["minAccuracy"]:
                    final_results[submission] = {"case": "rejected", "accuracy": results[submission]["accuracy"]}
                    self.logger.info(f"Removing submission with name '{submission}'")
                    delete_submission(self.DIRECTORY, submission, self.submission_file_types)
                else:
                    final_results[submission] = {
                        "case": "accepted",
                        "vtt": results[submission]["vtt"],
                        "metadata": sub2meta[submission],
                        "accuracy": results[submission]["accuracy"]
                    }
                    num_accepted_submissions += 1
            self.logger.info(f"Accepted {num_accepted_submissions} of {len(typed_submissions['normal'])} submission(s)")

        if "buzz" in typed_submissions:
            for submission in typed_submissions["buzz"]:
                final_results[submission] = {
                    "case": "accepted",
                    "metadata": sub2meta[submission]
                }
            self.logger.info(f"Received {len(typed_submissions['buzz'])} submission(s) for buzz-ins")

        if "answer" in typed_submissions:
            for submission in typed_submissions["answer"]:
                final_results[submission] = {
                    "case": "accepted",
                    "metadata": sub2meta[submission]
                }
            self.logger.info(f"Received {len(typed_submissions['answer'])} submission(s) for an answer to a question")
        return final_results

    def preprocess_submissions(self,
                               submissions: List[str],
                               sub2meta: Dict[str, Dict[str, str]]) -> Dict[str, Dict[str, Union[float, str]]]:
        """
        Return the accuracy and VTT data of each submission.

        :param submissions: The list of submissions to process
        :param sub2meta: A dictionary mapping submissions to metadata, which must contain the qb_id and should contain
                         either a sentenceId or a __sentenceIndex
        :return: A dictionary mapping submissions to results, which contain either an "accuracy" and "vtt" key or an
                 "err" key.
        """
        # TODO: Make into batches if possible.
        self.logger.info(f"Evaluating {len(submissions)} submission(s)...")

        num_finished_submissions = 0
        results = {}
        for submission in submissions:
            results[submission] = {}
            file_path = os.path.join(self.DIRECTORY, submission)
            wav_file_path = file_path + ".wav"
            # json_file_path = file_path + ".json"
            if sub2meta.get(submission) is None:
                self.logger.error(f"Metadata for submission '{submission}' not found. Skipping")
                results[submission]["err"] = "meta_not_found"
                continue
            metadata = sub2meta[submission]
            qid = metadata.get("qb_id")
            self.logger.debug(f"{type(qid)} qid = {qid!r}")

            if qid is None:
                self.logger.error(f"Question ID for submission '{submission}' not found. Skipping")
                results[submission]["err"] = "qid_not_found"
                continue

            query = {"qb_id": qid}

            sid = metadata.get("sentenceId")
            self.logger.debug(f"{type(sid)} sid = {sid!r}")
            if sid is None:
                self.logger.debug(f"Sentence ID for submission '{submission}' not found. Continuing without sentence ID")
                # self.logger.error(f"Sentence ID for submission {submission} not found. Skipping")
                # results[submission]["err"] = "sid_not_found"
                # continue
            else:
                query["sentenceId"] = sid

            self.logger.debug("Finding question in UnrecordedQuestions...")
            question = self.unrec_questions.find_one(query, {"transcript": 1, "tokenizations": 1})
            if question is None:
                self.logger.debug("Question not found in UnrecordedQuestions. Searching in RecordedQuestions...")
                question = self.rec_questions.find_one(query, {"transcript": 1, "tokenizations": 1})

            if question is None:
                self.logger.error("Question not found. Skipping submission")
                results[submission]["err"] = "sentence_not_found"
                continue

            r_transcript = question.get("transcript")

            if r_transcript is None:
                self.logger.error("Transcript not found. Skipping submission")
                results[submission]["err"] = "transcript_not_found"
                continue

            # __sentenceIndex is only included if no sentenceId is specified and the submission is part of a batch.
            if "__sentenceIndex" in metadata:
                self.logger.info("Attempting transcript segmentation...")
                if "tokenizations" in question:
                    slice_start, slice_end = question["tokenizations"][metadata["__sentenceIndex"]]
                    r_transcript = r_transcript[slice_start:slice_end]
                else:
                    self.logger.info("Could not segment transcript. Submission may not pass pre-screen")

            accuracy, vtt = self.get_accuracy_and_vtt(wav_file_path, r_transcript)
            if accuracy is None:
                results[submission]["err"] = "runtime_error"
                continue
            # accuracy = self.ACCURACY_CUTOFF
            # accuracy = random.random()

            num_finished_submissions += 1

            self.logger.info(f"Evaluated {num_finished_submissions}/{len(submissions)} submissions")
            self.logger.debug(f"Alignment for '{submission}' has accuracy {accuracy}")
            results[submission]["accuracy"] = accuracy
            results[submission]["vtt"] = vtt
        return results

    # ****************** HELPER METHODS *********************
    def get_accuracy_and_vtt(self, file_path: str, r_transcript: str):
        """
        Do a forced alignment and return the percentage of words aligned (or known) along with the VTT.

        :param file_path: The path to the WAV file
        :param r_transcript: The transcript to use as a reference
        :return: A tuple containing the accuracy and the VTT or None, None if a RuntimeError occurred.
        """
        try:
            alignment = forced_alignment.get_forced_alignment(file_path, r_transcript)
        except RuntimeError as e:
            self.logger.error(f"Encountered RuntimeError: {e}. Aborting")
            return None, None
        words = alignment.words
        # total_words = len(self.process_transcript(r_transcript))
        total_words = len(words)
        total_aligned_words = 0
        for word_data in words:
            unk = self.config["checkUnk"] and word_data.alignedWord == self.config["unkToken"]
            if word_data.case == "success" and not unk:
                total_aligned_words += 1
        vtt = vtt_conversion.gentle_alignment_to_vtt(words)

        return total_aligned_words / total_words, vtt

    def process_transcript(self, t: str) -> List[str]:
        """
        Return a list of words without punctuation or capitalization from a transcript.

        :param t: The transcript to process
        :return: A list containing every word without punctuation and purely lowercase
        """
        return re.split(self.whitespace_regex, re.sub(self.punc_regex, " ", t).lower())

    def get_metadata(self, submissions: List[str]) -> Dict[str, Dict[str, str]]:
        """
        Return the metadata of each submission (a JSON file) if available.

        :param submissions: The names of each submission
        :return: A dictionary mapping submission names to metadata for each submission that has metadata
        """
        sub2meta = {}
        for submission in submissions:
            submission_path = os.path.join(self.DIRECTORY, submission)
            if not os.path.exists(submission_path + ".json"):
                continue
            with open(submission_path + ".json", "r") as meta_f:
                sub2meta[submission] = bson.json_util.loads(meta_f.read())
        return sub2meta

    @staticmethod
    def categorize_submissions(submissions: List[str], sub2meta: Dict[str, dict]):
        """
        Divide the submissions by the recording type.

        :param submissions: The submission names
        :param sub2meta: A dictionary mapping submission names to metadata, which must contain a "recType" key
        :return: A dictionary mapping submission types to lists of submission names
        """
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


def delete_submission(directory: str, submission_name: str, file_types: List[str]):
    """
    Remove a submission from disk.

    :param directory: The directory to find the submission in
    :param submission_name: The name of the submission
    :param file_types: The file extensions to look for (must not start with a dot)
    """
    # TODO: Make it find all files with submission_name instead.
    submission_path = os.path.join(directory, submission_name)
    for ext in file_types:
        file_path = ".".join([submission_path, ext])
        if os.path.exists(file_path):
            os.remove(file_path)
