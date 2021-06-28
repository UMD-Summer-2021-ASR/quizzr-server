import json
import logging
import os
import random

import bson.json_util
from flask import Flask, request, render_template
import pymongo
from flask_cors import CORS

import gdrive_authentication

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# TODO: Implement rec_processing module through multiprocessing


class AutoIncrementer:
    def __init__(self):
        self.next = 0

    def get_next(self):
        next_num = self.next
        self.next += 1
        return next_num

    def reset(self):
        self.next = 0


class QuizzrServer:
    VERSION = "0.0.0"

    def __init__(self):
        # self.SERVER_DIR = os.environ['SERVER_DIR']
        self.SERVER_DIR = os.path.dirname(__file__)
        self.SECRET_DATA_DIR = os.path.join(self.SERVER_DIR, "privatedata")
        self.REC_DIR = os.path.join(self.SERVER_DIR, "recordings")

        with open(os.path.join(self.SECRET_DATA_DIR, "connectionstring")) as f:
            self.mongodb_client = pymongo.MongoClient(f.read())
        self.gdrive = gdrive_authentication.GDriveAuth(self.SECRET_DATA_DIR)

        self.database = self.mongodb_client.QuizzrDatabase
        self.users = self.database.Users
        self.rec_questions = self.database.RecordedQuestions
        self.unrec_questions = self.database.UnrecordedQuestions
        self.audio = self.database.Audio
        self.unproc_audio = self.database.UnprocessedAudio

        self.rec_question_ids = self.get_ids(self.rec_questions)
        self.unrec_question_ids = self.get_ids(self.unrec_questions)
        self.queue_id_gen = AutoIncrementer()

    def save_recording(self, file, metadata):
        file_name = self.get_file_name()
        file_path = os.path.join(self.REC_DIR, file_name)
        file.save(file_path + ".wav")
        with open(file_path + ".json", "w") as meta_f:
            json.dump(metadata, meta_f)
            meta_f.write(bson.json_util.dumps(metadata))
        return file_path + ".wav"

    def get_file_name(self):
        return str(self.queue_id_gen.get_next())

    @staticmethod
    def get_ids(collection):
        ids = []
        id_cursor = collection.find(None, {"_id": 1})
        for i, doc in enumerate(id_cursor):
            ids.append(doc["_id"])
        id_cursor.close()  # Do I really need to do this?
        return ids


app = Flask(__name__)
CORS(app)
qs = QuizzrServer()
# TODO: multiprocessing


@app.route("/upload", methods=["POST"])
def recording_listener():
    recording = request.files["audio"]
    # question_id = request.form["questionId"]
    # user_id = request.form["userId"]
    question_id = "60d0b89eba9c14e2eef1b791"
    user_id = "60d0ade3ba9c14e2eef1b78e"

    file_path = qs.save_recording(recording, {"questionId": bson.ObjectId(question_id), "userId": bson.ObjectId(user_id)})
    # upload_audio(file_path)
    # Notify the processor afterwards

    return render_template("submission.html")


@app.route("/answerquestion/", methods=["GET"])
def select_answer_question():
    if not qs.rec_question_ids:
        return {"err": "rec_not_found"}
    next_question = qs.rec_questions.find_one({"_id": random.choice(qs.rec_question_ids)})
    audio_cursor = qs.audio.find(
        {"_id": {"$in": next_question["recordings"]}},
        {"_id": 1, "vtt": 1, "accuracy": 1}
    )
    audio_cursor.sort("accuracy", pymongo.DESCENDING)
    audio = audio_cursor[0]
    audio_cursor.close()
    result = {"vtt": audio["vtt"], "fileId": audio["_id"]}
    return result


@app.route("/recordquestion/", methods=["GET"])
def select_record_question():
    if not qs.unrec_question_ids:
        return {"err": "unrec_not_found"}
    next_question_id = random.choice(qs.unrec_question_ids)
    next_question = qs.unrec_questions.find_one({"_id": next_question_id})
    result = next_question["transcript"]
    return {"transcript": result, "questionId": str(next_question_id)}


@app.route("/audio/unprocessed/", methods=["GET"])
def batch_unprocessed_audio():
    batch_size = 32
    logging.info(f"Finding a batch ({batch_size} max) of unprocessed audio documents...")
    audio_cursor = qs.unproc_audio.find(batch_size=batch_size)

    qid2entries = {}
    for audio_doc in audio_cursor:
        qid = audio_doc["questionId"]
        if qid not in qid2entries:
            qid2entries[qid] = []
        qid2entries[qid].append({"_id": audio_doc["_id"]})
    qids = list(qid2entries.keys())
    logging.info(f"Found {len(qids)} unprocessed audio document(s)")

    logging.info("Finding associated unrecorded questions...")
    unrec_cursor = qs.unrec_questions.find({"_id": {"$in": qids}})
    found_unrec_qids = []
    for question in unrec_cursor:
        qid = question["_id"]
        found_unrec_qids.append(qid)
        entries = qid2entries[qid]
        for entry in entries:
            entry["transcript"] = question["transcript"]
    logging.info(f"Found {len(found_unrec_qids)} unrecorded question(s)")

    found_rec_qids = [qid for qid in qids if qid not in found_unrec_qids]

    logging.info(f"Finding {len(found_rec_qids)} of {len(qids)} recorded questions...")
    rec_cursor = qs.rec_questions.find({"_id": {"$in": found_rec_qids}})
    rec_count = 0
    for question in rec_cursor:
        entries = qid2entries[question["_id"]]
        for entry in entries:
            entry["transcript"] = question["transcript"]
        rec_count += 1
    logging.info(f"Found {rec_count} recorded question(s)")
    results = []
    for entries in qid2entries.values():
        for entry in entries:
            results.append(entry)
    logging.debug(f"Final Results: {results}")
    return results


@app.route("/uploadtest/")  # Do not include in deployment.
def recording_listener_test():
    return render_template("uploadtest.html")


# DEPRECATED
def upload_audio(file_path):
    accuracy, vtt = qs.get_accuracy_and_vtt(file_path)
    file_id = qs.upload_to_gdrive(file_path)

    with open(file_path.split(".wav")[0] + ".json", "r") as meta_f:
        metadata_s = meta_f.read()
        metadata = bson.json_util.loads(metadata_s)
        question_id = metadata["questionId"]
        user_id = metadata["userId"]

    question_query = {"_id": question_id}
    unrecorded = True
    question = qs.unrec_questions.find_one_and_delete(question_query)
    if question is None:
        unrecorded = False
        question = qs.rec_questions.find_one(question_query)
    else:
        qs.unrec_question_ids.remove(question_id)

    audio_id = file_id
    audio_entry = {"_id": file_id, "vtt": vtt, "version": qs.VERSION, "user": user_id, "question": question_id,
                   "accuracy": accuracy}
    qs.audio.insert_one(audio_entry)

    if unrecorded:
        question["recordings"] = [audio_id]
        qs.rec_questions.insert_one(question)
        qs.rec_question_ids.append(question_id)
    else:
        qs.rec_questions.update_one(question_query, {"$push": {"recordings": audio_id}})

    user_query = {"_id": user_id}
    qs.users.update_one(user_query, {"$push": {"recordedAudios": audio_id}})


if __name__ == "__main__":
    pass
