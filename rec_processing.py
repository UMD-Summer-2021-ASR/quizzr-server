from queue import Full, Queue
from contextlib import closing
import logging
import multiprocessing
import os
import pprint
import re

import sys
import time
import wave
from typing import List, Dict, Union
from uuid import uuid4

# import audioevaluator.evaluator
import bson.json_util
from pymongo.database import Database

import forced_alignment
import vtt_conversion
from tpm import QuizzrTPM


BATCH_SUBMISSION_REGEX = re.compile(r"(.+)b\d$")
PUNC_REGEX = re.compile(r"[.?!,;:\"\-]")
WHITESPACE_REGEX = re.compile(r"\s+")


class QuizzrWatcher:
    """A class for polling a queue directory and running a function when submissions are found. A submission must
    consist of one or more files sharing a base name."""
    def __init__(self, watch_dir: str, error_dir: str, func, queue: Queue, interval: float = 2,
                 poll_size_limit: int = 32, submission_file_types: list = None, logger=None):
        """
        Create an instance of this class in preparation for execution and make the required directories.

        :param watch_dir: The queue directory to poll
        :param error_dir: The directory to store queued submissions when an unhandled error occurs
        :param func: The function to execute when files are found. Must accept a list of strings as the first argument
                     and must return an iterable containing items
        :param queue: The Queue to use for inserting the results of "func"
        :param interval: The poll interval
        :param poll_size_limit: The maximum number of files to collect at once
        :param submission_file_types: The file extensions each submission can have (do not start with a dot)
        :param logger: (optional) An instance of the logging.Logger class
        """
        self.done = False
        self.queue = queue
        self.watch_dir = watch_dir
        self.error_dir = error_dir
        self.poll_size_limit = poll_size_limit
        self.func = func
        self.interval = interval
        self.submission_file_types = submission_file_types or ["wav", "json"]
        self.logger = logger or logging.getLogger(__name__)
        os.makedirs(self.watch_dir, exist_ok=True)
        os.makedirs(self.error_dir, exist_ok=True)
        # atexit.register(self.exit_handler)
        # signal.signal(signal.SIGINT, self.signal_handler)

    # def exit_handler(self):
    #     print('Exit handler executed!')

    # def signal_handler(self, signal, frame):
    #     self.done = True

    def execute(self):
        """Start running the main loop of the process."""
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
        Get up to ``size_limit`` submission names.

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
    """A class for pre-screening and uploading submissions"""
    def __init__(self, qtpm: QuizzrTPM, directory: str, config: dict, submission_file_types: List[str] = None,
                 logger=None):
        """
        Instantiate the class for use. Requires ``<directory>/queue`` to be present

        :param qtpm: An instance of the QuizzrTPM class
        :param directory: The working directory of the processor
        :param config: The configuration to use when instantiating QuizzrProcessor (requires the "minAccuracy",
                       "checkUnk", and "unkToken" keys)
        :param submission_file_types: The possible file extensions of a submission (do not start with a dot)
        :param logger: (optional) An instance of the logging.Logger class
        """
        self.qtpm = qtpm
        if submission_file_types is None:
            self.submission_file_types = ["wav", "json"]
        else:
            self.submission_file_types = submission_file_types
        self.directory = directory  # May be used for the Montreal Forced Aligner
        self.rec_directory = os.path.join(self.directory, "queue")
        # if not os.path.exists(self.rec_directory):
        #     os.makedirs(self.rec_directory)
        self.logger = logger or logging.getLogger(__name__)
        self.qp = QuizzrProcessor(qtpm.database, self.rec_directory, config, submission_file_types, self.logger)

    def execute(self, submissions: List[str]):
        """
        Pre-screen and delete the given submissions, uploading submissions that passed the pre-screening to Firebase and
        MongoDB.

        Precondition: WAV and JSON files exist in ``<self.directory>/queue`` for each submission name provided.

        :param submissions: A list of submission names to pre-screen
        :return: A list of dictionaries containing the submission "name", the "case", and the "err" when the "case" is
                 "err"
        """
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

        sub2meta = {}
        sub2vtt = {}
        sub2score = {}

        for submission in results:
            doc = results[submission]
            if doc["case"] == "accepted":
                sub2meta[submission] = doc["metadata"]
                if "vtt" in doc:
                    sub2vtt[submission] = doc["vtt"]
                if "score" in doc:
                    sub2score[submission] = doc["score"]

        # Upload submission metadata to MongoDB
        sub2blob = {os.path.splitext(file)[0]: file2blob[file] for file in file2blob}
        self.qtpm.mongodb_insert_submissions(
            sub2blob=sub2blob,
            sub2meta=sub2meta,
            sub2vtt=sub2vtt,
            sub2score=sub2score
        )

        # Get summary and remove submissions
        summary = []

        for submission in results:
            end_result = {"name": submission, "case": results[submission]["case"]}
            if end_result["case"] == "err":
                end_result["err"] = results[submission]["err"]
            summary.append(end_result)
            self.logger.info(f"Removing submission with name '{submission}'")
            delete_submission(self.rec_directory, submission, self.submission_file_types)

        return summary


class QuizzrProcessor:
    """A class for pre-screening submissions"""
    def __init__(self, database: Database, directory: str, config: dict, submission_file_types: List[str] = None,
                 logger=None):
        """
        Instantiate the class for use. Requires a connection to MongoDB.

        :param database: A pymongo.database.Database instance
        :param directory: The working directory for pre-screening
        :param config: A dictionary containing the the "minAccuracy", "checkUnk", and "unkToken" keys
        :param submission_file_types: The possible file extensions of a submission (do not start with a dot)
        :param logger: (optional) An instance of the logging.Logger class
        """
        self.logger = logger or logging.getLogger(__name__)
        self.submission_file_types = submission_file_types or ["wav", "json"]
        self.config = config
        self.DIRECTORY = directory

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
                    batch_uuid = str(uuid4())
                    for i, batch_item in enumerate(submission):
                        sub2meta[batch_item]["batchUUID"] = batch_uuid
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
                                "score": result["score"][i],
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
                    else:
                        final_results[submission] = {
                            "case": "accepted",
                            "vtt": result["vtt"],
                            "score": result["score"],
                            "metadata": sub2meta[submission],
                            "accuracy": result["accuracy"]
                        }
                        final_results[submission]["metadata"]["duration"] = self.get_duration(submission)
                        num_accepted_submissions += 1
            self.logger.debug(f"final_results = {pprint.pformat(final_results)}")

            # FIXME: This number is currently inaccurate with batch submissions.
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
                         either a sentenceId or a tokenizationId
        :return: A dictionary mapping submissions to results, which contain either an "accuracy", "score", and "vtt" key
                 or an "err" key.
        """
        # TODO: Make pre-screening of multiple submissions happen in parallel / asynchronously (benchmark first).
        #   Parallel = multiprocessing.Pool
        #   Asynchronous = asyncio
        # TODO: Add timeout as a workaround to forced alignments sometimes blocking indefinitely.
        self.logger.info(f"Evaluating {len(submissions)} submission(s)...")

        num_finished_submissions = 0
        results = []
        for submission in submissions:
            if type(submission) is list:
                total_accuracy_fraction = [0, 0]
                vtt_list = []
                score_list = []
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
                    score_list.append(result["score"])

                if batch_has_error:
                    continue

                accuracy = total_accuracy_fraction[0] / total_accuracy_fraction[1]
                results.append((submission, {
                    "accuracy": accuracy,
                    "vtt": vtt_list,
                    "score": score_list
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

                # noinspection PyUnresolvedReferences
                accuracy = result["accuracyFraction"][0] / result["accuracyFraction"][1]
                results.append((submission, {"accuracy": accuracy, "vtt": result["vtt"], "score": result["score"]}))

            num_finished_submissions += 1

            self.logger.info(f"Evaluated {num_finished_submissions}/{len(submissions)} submissions")
            self.logger.debug(f"Alignment for '{submission}' has accuracy {accuracy}")
        return results

    def preprocess_one_submission(self, submission: str, metadata: dict):
        """
        Pre-screen a single submission with the given metadata and return the accuracy fraction and VTT in a dictionary.

        :param submission: The name of the submission to pre-screen
        :param metadata: The metadata associated with the submission
        :return: A dictionary containing the "accuracyFraction" (a tuple representing the accuracy as a fraction), the
                 alignment as a "vtt", and the "score" (the "wer", "mer", and "wil" all in one variable)
        """
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

        # tokenizationId is only included if no sentenceId is specified and the submission is part of a batch.
        if "tokenizationId" in metadata:
            self.logger.info("Attempting transcript segmentation...")
            if "tokenizations" in question:
                slice_start, slice_end = question["tokenizations"][metadata["tokenizationId"]]
                r_transcript = r_transcript[slice_start:slice_end]
            else:
                self.logger.info("Could not segment transcript. Submission may not pass pre-screen")

        self.logger.debug(f"r_transcript = {r_transcript!r}")

        try:
            aligned_words, num_words, vtt = self.get_accuracy_and_vtt(wav_file_path, r_transcript)
        except RuntimeError as e:
            self.logger.error(f"Encountered RuntimeError: {e}. Aborting")
            return {"err": "runtime_error", "extra": str(e)}

        # Get WER, MER, and WIL from an automatic speech recognizer
        # FIXME: PyTorch has issues when working with CUDA and forked processes at the same time
        # asr_score = audioevaluator.evaluator.evaluate_audio(wav_file_path, r_transcript)
        asr_score = {}

        return {"accuracyFraction": (aligned_words, num_words), "vtt": vtt, "score": asr_score}
        # return {"accuracyFraction": (aligned_words, num_words), "vtt": vtt, "score": {"wer": 0, "mer": 0, "wil": 0}}

    # ****************** HELPER METHODS *********************
    def get_accuracy_and_vtt(self, file_path: str, r_transcript: str):
        """
        Do a forced alignment and return the number of aligned words, the total number of words, and the VTT as a tuple.

        :param file_path: The path to the WAV file
        :param r_transcript: The transcript to use as a reference
        :return: A tuple containing the accuracy and the VTT
        """
        alignment = forced_alignment.get_forced_alignment(file_path, r_transcript)
        words = alignment.words
        total_words = len(words)
        aligned_words = 0
        for word_data in words:
            unk = self.config["checkUnk"] and word_data.alignedWord == self.config["unkToken"]
            if word_data.success() and not unk:
                aligned_words += 1
        realigned_alignment = vtt_conversion.realign_alignment(alignment)
        vtt = vtt_conversion.gentle_alignment_to_vtt(realigned_alignment.words)
        # self.logger.debug(f"vtt = {vtt}")

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
        """
        Get the duration of the WAV file of a submission in seconds.

        :param submission: The name of the submission
        :return: The duration of the associated WAV file in seconds
        """
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
    def bundle_submissions(submissions: List[str]) -> List[Union[List[str], str]]:
        """
        Return a list where submissions that end with "b<number>" are grouped by their base name and where standalone
        submissions are left as single strings.

        :param submissions: The submissions to "bundle"
        :return: A list where submissions that end with "b<number>" are grouped by their base name and where standalone
        submissions are left as single strings
        """
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


def start_watcher(db_name, tpm_config, firebase_app_specifier, rec_dir, proc_config, queue, queue_dir=None,
                  error_dir=None, submission_file_types=None, logger=None):
    """
    Initialize and run the main loop of a QuizzrWatcher

    :param db_name: See QuizzrTPM.__init__() for more details
    :param tpm_config: See QuizzrTPM.__init__() for more details
    :param firebase_app_specifier: See QuizzrTPM.__init__() for more details
    :param rec_dir: See QuizzrProcessorHead.__init__() for more details
    :param proc_config: See QuizzrProcessorHead.__init__() for more details
    :param queue: See QuizzrWatcher.__init__() for more details
    :param queue_dir: (optional) See QuizzrWatcher.__init__() for more details
    :param error_dir: (optional) See QuizzrWatcher.__init__() for more details
    :param submission_file_types: (optional) The possible file extensions of a submission (do not start with a dot)
    :param logger: (optional) An instance of the logging.Logger class
    """
    logger = logger or logging.getLogger(__name__)
    queue_dir = queue_dir or os.path.join(rec_dir, "queue")
    error_dir = error_dir or os.path.join(rec_dir, "_error")
    logger.info("Initializing...")
    logger.debug("Instantiating QuizzrProcessorHead...")
    qtpm = QuizzrTPM(db_name, tpm_config, firebase_app_specifier, logger.getChild("qtpm"))
    qph = QuizzrProcessorHead(qtpm, rec_dir, proc_config, submission_file_types, logger.getChild("qp"))
    logger.debug("Finished instantiating QuizzrProcessorHead")
    logger.debug("Instantiating QuizzrWatcher...")
    qw = QuizzrWatcher(queue_dir, error_dir, qph.execute, queue, logger=logger)
    logger.debug("Finished instantiating QuizzrWatcher")
    logger.info("Finished initialization. Entering main loop")
    qw.execute()


# TODO: Change this
def main():
    """Outdated. Do not use."""
    database = os.environ["Q_DATABASE"]
    rec_dir = os.path.expanduser("~/quizzr_server/storage/queue")
    qtpm = QuizzrTPM(database, {
        "BLOB_ROOT": "development",
        "VERSION": "0.2.0",
        "BLOB_NAME_LENGTH": 32
    }, os.environ["SECRET_DIR"])
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
