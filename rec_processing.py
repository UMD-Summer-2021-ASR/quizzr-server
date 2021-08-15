import atexit
from queue import Full
from contextlib import closing
import logging
import multiprocessing
import os
import pprint
import re
import signal

import sys
import time
import wave
from typing import List, Dict, Union

import bson.json_util
from pymongo.database import Database

import forced_alignment
import vtt_conversion
from sv_api import QuizzrAPISpec
from tpm import QuizzrTPM

# logging.basicConfig(level=os.environ.get("QUIZZR_LOG") or "DEBUG")

BATCH_SUBMISSION_REGEX = re.compile(r"(.+)b\d$")
PUNC_REGEX = re.compile(r"[.?!,;:\"\-]")
WHITESPACE_REGEX = re.compile(r"\s+")


class QuizzrWatcher:
    def __init__(self, watch_dir: str, error_dir: str, func, queue, interval: float = 2, poll_size_limit: int = 32,
                 submission_file_types: list = None):
        self.done = False
        self.queue = queue
        self.watch_dir = watch_dir
        self.error_dir = error_dir
        self.poll_size_limit = poll_size_limit
        self.func = func
        self.interval = interval
        self.submission_file_types = submission_file_types or ["wav", "json"]
        self.logger = logging.getLogger(__name__)
        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.error_dir, exist_ok=True)
        atexit.register(self.exit_handler)
        # signal.signal(signal.SIGINT, self.signal_handler)

    def exit_handler(self):
        print('Exit handler executed!')

    # def signal_handler(self, signal, frame):
    #     self.done = True

    def execute(self):
        while not self.done:
            queued_submissions = self.queue_submissions(self.watch_dir, self.poll_size_limit)
            if queued_submissions:
                try:
                    results = self.func(queued_submissions)
                except Exception as e:
                    self.logger.exception("Encountered an error during pre-screening")
                    for t in self.submission_file_types:
                        for sub_name in queued_submissions:
                            sub_path = ".".join([os.path.join(self.watch_dir, sub_name), t])
                            error_path = ".".join([os.path.join(self.error_dir, sub_name), t])
                            if os.path.exists(sub_path):
                                # FIXME: Will raise FileExistsError on Windows
                                os.rename(sub_path, error_path)
                    try:
                        self.queue.put((queued_submissions, e))
                    except Full:
                        self.logger.error("Could not push error to queue")
                else:
                    for item in results:
                        try:
                            self.queue.put(item)
                        except Full:
                            self.logger.error("Queue is full. Aborting")
                            break
            try:
                time.sleep(self.interval)
            except KeyboardInterrupt:
                pass

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


class QuizzrProcessorHead:
    def __init__(self, qtpm: QuizzrTPM, directory: str, config: dict, submission_file_types: List[str] = None):
        self.qtpm = qtpm
        if submission_file_types is None:
            self.submission_file_types = ["wav", "json"]
        else:
            self.submission_file_types = submission_file_types
        self.directory = directory  # May be used for the Montreal Forced Aligner
        self.rec_directory = os.path.join(self.directory, "queue")
        self.qp = QuizzrProcessor(qtpm.database, self.rec_directory, config, submission_file_types)
        if not os.path.exists(self.rec_directory):
            os.makedirs(self.rec_directory)
        self.logger = logging.getLogger(__name__)

    def execute(self, submissions: List[str]):
        results = self.qp.pick_submissions(submissions)

        # Split by recType
        self.logger.info("Preparing results for upload...")
        file_paths = {}
        for submission in results:
            file_path = os.path.join(self.rec_directory, submission) + ".wav"
            if results[submission]["case"] == "accepted":
                sub_rec_type = results[submission]["metadata"]["recType"]
                if sub_rec_type not in file_paths:
                    file_paths[sub_rec_type] = []
                file_paths[sub_rec_type].append(file_path)

        self.logger.debug(f"file_paths = {file_paths!r}")

        # Upload files
        file2blob = {}
        for rt, paths in file_paths.items():  # Organize by recType
            file2blob.update(self.qtpm.upload_many(paths, rt))

        self.logger.debug(f"file2blob = {file2blob!r}")

        # sub2blob = {os.path.splitext(file)[0]: file2blob[file] for file in file2blob}
        sub2meta = {}
        sub2vtt = {}

        for submission in results:
            doc = results[submission]
            if doc["case"] == "accepted":
                sub2meta[submission] = doc["metadata"]
                if "vtt" in doc:
                    sub2vtt[submission] = doc.get("vtt")

        # Upload submission metadata to MongoDB
        self.qtpm.mongodb_insert_submissions(
            sub2blob={os.path.splitext(file)[0]: file2blob[file] for file in file2blob},
            sub2meta=sub2meta,
            sub2vtt=sub2vtt
        )

        summary = []

        for submission in results:
            self.logger.info(f"Removing submission with name '{submission}'")
            delete_submission(self.rec_directory, submission, self.submission_file_types)
            end_result = {"name": submission, "case": results[submission]["case"]}
            if end_result["case"] == "err":
                end_result["err"] = results[submission]["err"]
            summary.append(end_result)

        return summary


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
            preprocess_list = QuizzrProcessor.bundle_submissions(typed_submissions["normal"])
            results = self.preprocess_submissions(preprocess_list, sub2meta)
            # for submission in typed_submissions['normal']:
            for submission, result in results:
                if type(submission) is list:
                    for i, batch_item in enumerate(submission):
                        if "err" in result:
                            if result["submissionName"] == batch_item:
                                final_results[batch_item] = {"case": "err", "err": result["err"]}
                            else:
                                final_results[batch_item] = {"case": "rejected", "reason": "error_in_batch"}
                        elif result["accuracy"] < self.config["minAccuracy"]:
                            final_results[batch_item] = {
                                "case": "rejected",
                                "accuracy": result["accuracy"],
                                "metadata": sub2meta[batch_item]
                            }
                        else:
                            final_results[batch_item] = {
                                "case": "accepted",
                                "vtt": result["vtt"][i],
                                "metadata": sub2meta[batch_item],
                                "accuracy": result["accuracy"]
                            }
                            final_results[batch_item]["metadata"]["duration"] = self.get_duration(batch_item)
                else:
                    if "err" in result:
                        final_results[submission] = {"case": "err", "err": result["err"]}
                    elif result["accuracy"] < self.config["minAccuracy"]:
                        final_results[submission] = {
                            "case": "rejected",
                            "accuracy": result["accuracy"],
                            "metadata": sub2meta[submission]
                        }
                        # self.logger.info(f"Removing submission with name '{submission}'")
                        # delete_submission(self.DIRECTORY, submission, self.submission_file_types)
                    else:
                        final_results[submission] = {
                            "case": "accepted",
                            "vtt": result["vtt"],
                            "metadata": sub2meta[submission],
                            "accuracy": result["accuracy"]
                        }
                        final_results[submission]["metadata"]["duration"] = self.get_duration(submission)
                        num_accepted_submissions += 1
            self.logger.debug(f"final_results = {pprint.pformat(final_results)}")

            # NOTE: This number is currently inaccurate with batch submissions.
            self.logger.info(f"Accepted {num_accepted_submissions} of {len(typed_submissions['normal'])} submission(s)")

        if "buzz" in typed_submissions:
            for submission in typed_submissions["buzz"]:
                final_results[submission] = {
                    "case": "accepted",
                    "metadata": sub2meta[submission]
                }
                final_results[submission]["metadata"]["duration"] = self.get_duration(submission)
            self.logger.info(f"Received {len(typed_submissions['buzz'])} submission(s) for buzz-ins")

        if "answer" in typed_submissions:
            for submission in typed_submissions["answer"]:
                final_results[submission] = {
                    "case": "accepted",
                    "metadata": sub2meta[submission]
                }
                final_results[submission]["metadata"]["duration"] = self.get_duration(submission)
            self.logger.info(f"Received {len(typed_submissions['answer'])} submission(s) for an answer to a question")
        return final_results

    def preprocess_submissions(self,
                               submissions: List[Union[List[str], str]],
                               sub2meta: Dict[str, Dict[str, str]]):
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
        # results = {}
        # for submission in submissions:
        #     results[submission] = {}
        #     file_path = os.path.join(self.DIRECTORY, submission)
        #     wav_file_path = file_path + ".wav"
        #     # json_file_path = file_path + ".json"
        #     if submission not in sub2meta:
        #         self.logger.error(f"Metadata for submission '{submission}' not found. Skipping")
        #         results[submission]["err"] = "meta_not_found"
        #         continue
        #     metadata = sub2meta[submission]
        #     qid = metadata.get("qb_id")
        #     self.logger.debug(f"{type(qid)} qid = {qid!r}")
        #
        #     if qid is None:
        #         self.logger.error(f"Question ID for submission '{submission}' not found. Skipping")
        #         results[submission]["err"] = "qid_not_found"
        #         continue
        #
        #     query = {"qb_id": qid}
        #
        #     sid = metadata.get("sentenceId")
        #     self.logger.debug(f"{type(sid)} sid = {sid!r}")
        #     if sid is None:
        #         self.logger.debug(f"Sentence ID for submission '{submission}' not found. Continuing without sentence ID")
        #         # self.logger.error(f"Sentence ID for submission {submission} not found. Skipping")
        #         # results[submission]["err"] = "sid_not_found"
        #         # continue
        #     else:
        #         query["sentenceId"] = sid
        #
        #     self.logger.debug("Finding question in UnrecordedQuestions...")
        #     question = self.unrec_questions.find_one(query, {"transcript": 1, "tokenizations": 1})
        #     if question is None:
        #         self.logger.debug("Question not found in UnrecordedQuestions. Searching in RecordedQuestions...")
        #         question = self.rec_questions.find_one(query, {"transcript": 1, "tokenizations": 1})
        #
        #     if question is None:
        #         self.logger.error("Question not found. Skipping submission")
        #         results[submission]["err"] = "sentence_not_found"
        #         continue
        #
        #     r_transcript = question.get("transcript")
        #     self.logger.debug(f"r_transcript = {r_transcript!r}")
        #
        #     if r_transcript is None:
        #         self.logger.error("Transcript not found. Skipping submission")
        #         results[submission]["err"] = "transcript_not_found"
        #         continue
        #
        #     # __sentenceIndex is only included if no sentenceId is specified and the submission is part of a batch.
        #     if "__sentenceIndex" in metadata:
        #         self.logger.info("Attempting transcript segmentation...")
        #         if "tokenizations" in question:
        #             slice_start, slice_end = question["tokenizations"][metadata["__sentenceIndex"]]
        #             r_transcript = r_transcript[slice_start:slice_end]
        #         else:
        #             self.logger.info("Could not segment transcript. Submission may not pass pre-screen")
        #
        #     self.logger.debug(f"r_transcript = {r_transcript!r}")
        #
        #     aligned_words, num_words, vtt = self.get_accuracy_and_vtt(wav_file_path, r_transcript)
        #     accuracy = aligned_words / num_words
        #     if accuracy is None:
        #         results[submission]["err"] = "runtime_error"
        #         continue
        #     # accuracy = self.ACCURACY_CUTOFF
        #     # accuracy = random.random()
        #
        #     results[submission]["accuracy"] = accuracy
        #     results[submission]["vtt"] = vtt
        #
        #     num_finished_submissions += 1
        #
        #     self.logger.info(f"Evaluated {num_finished_submissions}/{len(submissions)} submissions")
        #     self.logger.debug(f"Alignment for '{submission}' has accuracy {accuracy}")
        results = []
        for submission in submissions:
            # file_path = os.path.join(self.DIRECTORY, submission)
            # wav_file_path = file_path + ".wav"
            # json_file_path = file_path + ".json"
            # if submission not in sub2meta:
            #     self.logger.error(f"Metadata for submission '{submission}' not found. Skipping")
            #     results.append((submission, {"err": "metadata_not_found"}))
            #     continue
            # metadata = sub2meta[submission]
            # qid = metadata.get("qb_id")
            # self.logger.debug(f"{type(qid)} qid = {qid!r}")
            #
            # if qid is None:
            #     self.logger.error(f"Question ID for submission '{submission}' not found. Skipping")
            #     results.append((submission, {"err": "qid_not_found"}))
            #     continue
            #
            # query = {"qb_id": qid}
            #
            # sid = metadata.get("sentenceId")
            # self.logger.debug(f"{type(sid)} sid = {sid!r}")
            # if sid is None:
            #     self.logger.debug(
            #         f"Sentence ID for submission '{submission}' not found. Continuing without sentence ID")
            #     # self.logger.error(f"Sentence ID for submission {submission} not found. Skipping")
            #     # results[submission]["err"] = "sid_not_found"
            #     # continue
            # else:
            #     query["sentenceId"] = sid
            #
            # self.logger.debug("Finding question in UnrecordedQuestions...")
            # question = self.unrec_questions.find_one(query, {"transcript": 1, "tokenizations": 1})
            # if question is None:
            #     self.logger.debug("Question not found in UnrecordedQuestions. Searching in RecordedQuestions...")
            #     question = self.rec_questions.find_one(query, {"transcript": 1, "tokenizations": 1})
            #
            # if question is None:
            #     self.logger.error("Question not found. Skipping submission")
            #     results.append((submission, {"err": "sentence_not_found"}))
            #     continue
            #
            # r_transcript = question.get("transcript")
            # self.logger.debug(f"r_transcript = {r_transcript!r}")
            #
            # if r_transcript is None:
            #     self.logger.error("Transcript not found. Skipping submission")
            #     results.append((submission, {"err": "transcript_not_found"}))
            #     continue
            #
            # # __sentenceIndex is only included if no sentenceId is specified and the submission is part of a batch.
            # if "__sentenceIndex" in metadata:
            #     self.logger.info("Attempting transcript segmentation...")
            #     if "tokenizations" in question:
            #         slice_start, slice_end = question["tokenizations"][metadata["__sentenceIndex"]]
            #         r_transcript = r_transcript[slice_start:slice_end]
            #     else:
            #         self.logger.info("Could not segment transcript. Submission may not pass pre-screen")
            #
            # self.logger.debug(f"r_transcript = {r_transcript!r}")
            #
            # try:
            #     aligned_words, num_words, vtt = self.get_accuracy_and_vtt(wav_file_path, r_transcript)
            # except RuntimeError as e:
            #     self.logger.error(f"Encountered RuntimeError: {e}. Aborting")
            #     results.append((submission, {"err": "runtime_error", "extra": str(e)}))
            #     continue
            # accuracy = aligned_words / num_words
            # # if accuracy is None:
            # #     results[submission]["err"] = "runtime_error"
            # #     continue
            # # accuracy = self.ACCURACY_CUTOFF
            # # accuracy = random.random()
            #
            # results.append((submission, {"accuracy": accuracy, "vtt": vtt}))
            if type(submission) is list:
                total_accuracy_fraction = [0, 0]
                vtt_list = []
                batch_has_error = False
                for batch_item in submission:
                    if batch_item not in sub2meta:
                        self.logger.error(f"Metadata for submission '{batch_item}' not found. Skipping batch")
                        results.append((submission, {"err": "metadata_not_found", "submissionName": batch_item}))
                        batch_has_error = True
                        break
                    result = self.preprocess_one_submission(batch_item, sub2meta[batch_item])
                    if "err" in result:
                        self.logger.error(f"Error encountered in batch {submission}. Skipping batch")
                        results.append((submission, {**result, "submissionName": batch_item}))
                        batch_has_error = True
                        break
                    total_accuracy_fraction[0] += result["accuracyFraction"][0]
                    total_accuracy_fraction[1] += result["accuracyFraction"][1]
                    vtt_list.append(result["vtt"])

                if batch_has_error:
                    continue

                accuracy = total_accuracy_fraction[0] / total_accuracy_fraction[1]
                results.append((submission, {
                    "accuracy": accuracy,
                    "vtt": vtt_list
                }))
            else:
                if submission not in sub2meta:
                    self.logger.error(f"Metadata for submission '{submission}' not found. Skipping")
                    results.append((submission, {"err": "metadata_not_found"}))
                    continue
                result = self.preprocess_one_submission(submission, sub2meta[submission])
                if "err" in result:
                    self.logger.error(f"Error encountered with submission '{submission}'. Skipping")
                    results.append((submission, result))
                    continue
                accuracy = result["accuracyFraction"][0] / result["accuracyFraction"][1]
                results.append((submission, {"accuracy": accuracy, "vtt": result["vtt"]}))

            num_finished_submissions += 1

            self.logger.info(f"Evaluated {num_finished_submissions}/{len(submissions)} submissions")
            self.logger.debug(f"Alignment for '{submission}' has accuracy {accuracy}")
        return results

    def preprocess_one_submission(self, submission: str, metadata: dict):
        file_path = os.path.join(self.DIRECTORY, submission)
        wav_file_path = file_path + ".wav"
        qid = metadata.get("qb_id")
        self.logger.debug(f"{type(qid)} qid = {qid!r}")

        if qid is None:
            self.logger.error(f"Question ID for submission '{submission}' not found. Skipping")
            return {"err": "qid_not_found"}

        query = {"qb_id": qid}

        sid = metadata.get("sentenceId")
        self.logger.debug(f"{type(sid)} sid = {sid!r}")
        if sid is None:
            self.logger.debug(
                f"Sentence ID for submission '{submission}' not found. Continuing without sentence ID")
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
            return {"err": "sentence_not_found"}

        r_transcript = question.get("transcript")
        self.logger.debug(f"r_transcript = {r_transcript!r}")

        if r_transcript is None:
            self.logger.error("Transcript not found. Skipping submission")
            return {"err": "transcript_not_found"}

        # __sentenceIndex is only included if no sentenceId is specified and the submission is part of a batch.
        if "__sentenceIndex" in metadata:
            self.logger.info("Attempting transcript segmentation...")
            if "tokenizations" in question:
                slice_start, slice_end = question["tokenizations"][metadata["__sentenceIndex"]]
                r_transcript = r_transcript[slice_start:slice_end]
            else:
                self.logger.info("Could not segment transcript. Submission may not pass pre-screen")

        self.logger.debug(f"r_transcript = {r_transcript!r}")

        try:
            aligned_words, num_words, vtt = self.get_accuracy_and_vtt(wav_file_path, r_transcript)
        except RuntimeError as e:
            self.logger.error(f"Encountered RuntimeError: {e}. Aborting")
            return {"err": "runtime_error", "extra": str(e)}

        return {"accuracyFraction": (aligned_words, num_words), "vtt": vtt}

    # ****************** HELPER METHODS *********************
    def get_accuracy_and_vtt(self, file_path: str, r_transcript: str):
        """
        Do a forced alignment and return the number of aligned words, the total number of words, and the VTT as a tuple.

        :param file_path: The path to the WAV file
        :param r_transcript: The transcript to use as a reference
        :return: A tuple containing the accuracy and the VTT or None, None if a RuntimeError occurred.
        """
        alignment = forced_alignment.get_forced_alignment(file_path, r_transcript)
        words = alignment.words
        # total_words = len(self.process_transcript(r_transcript))
        total_words = len(words)
        aligned_words = 0
        for word_data in words:
            unk = self.config["checkUnk"] and word_data.alignedWord == self.config["unkToken"]
            if word_data.case == "success" and not unk:
                aligned_words += 1
        vtt = vtt_conversion.gentle_alignment_to_vtt(words)

        return aligned_words, total_words, vtt

    def process_transcript(self, t: str) -> List[str]:
        """
        Return a list of words without punctuation or capitalization from a transcript.

        :param t: The transcript to process
        :return: A list containing every word without punctuation and purely lowercase
        """
        return re.split(WHITESPACE_REGEX, re.sub(PUNC_REGEX, " ", t).lower())

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

    def get_duration(self, submission: str) -> float:
        submission_path = os.path.join(self.DIRECTORY, submission) + ".wav"
        with closing(wave.open(submission_path, "r")) as f:
            duration = f.getnframes() / f.getframerate()
        return duration

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

    @staticmethod
    def bundle_submissions(submissions):
        """Return a list where submissions that end with "b<number>" are grouped by their base name and where standalone
        submissions are left as single strings."""
        bundle_list = []
        next_bundle = []
        prev_token = None

        for i, submission in enumerate(submissions):
            match = re.match(BATCH_SUBMISSION_REGEX, submission)

            if match:
                token = match.group(1)
                # Ensure that batches are in separate lists
                if prev_token and prev_token != token and next_bundle:
                    bundle_list.append(next_bundle)
                    next_bundle = []
                next_bundle.append(submission)
                prev_token = token
            elif next_bundle:
                # This isn't a match
                bundle_list.append(next_bundle)
                next_bundle = []

            if i + 1 >= len(submissions) and next_bundle:
                bundle_list.append(next_bundle)
                next_bundle = []

            if not match:
                bundle_list.append(submission)

        return bundle_list

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


def start_watcher(db_name, tpm_config, firebase_app_specifier, api, rec_dir, queue_dir, error_dir, proc_config, queue, submission_file_types=None):
    # logger.info("Initializing pre-screening program...")
    # logger.debug("Instantiating QuizzrProcessorHead...")
    qtpm = QuizzrTPM(db_name, tpm_config, api, firebase_app_specifier)
    qph = QuizzrProcessorHead(
        qtpm,
        rec_dir,
        proc_config,
        submission_file_types
    )
    # logger.debug("Finished instantiating QuizzrProcessorHead")
    # logger.debug("Instantiating QuizzrWatcher...")
    qw = QuizzrWatcher(queue_dir, error_dir, qph.execute, queue)
    # logger.debug("Finished instantiating QuizzrWatcher")
    # logger.debug("Starting process...")
    qw.execute()


# TODO: Change this
def main():
    database = os.environ["Q_DATABASE"]
    rec_dir = os.path.expanduser("~/quizzr_server/storage/queue")
    qtpm = QuizzrTPM(database, {
        "BLOB_ROOT": "development",
        "VERSION": "mfa_branch",
        "BLOB_NAME_LENGTH": 32
    }, QuizzrAPISpec(os.path.expanduser("~/PycharmProjects/quizzr-server/reference/backend.yaml")),
                     os.environ["SECRET_DIR"])
    qph = QuizzrProcessorHead(qtpm, rec_dir, {
            "checkUnk": True,
            "unkToken": "<unk>",
            "minAccuracy": 0.5,
            "queueLimit": 32
        }, ["wav", "json", "vtt"])
    qw = QuizzrWatcher(rec_dir, rec_dir, qph.execute, multiprocessing.Queue())
    qw.execute()


if __name__ == '__main__':
    main()
