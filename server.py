import json
import os
import random
from datetime import datetime

from bson import ObjectId
from flask import Flask, request, render_template
import pymongo
from flask_cors import CORS
from googleapiclient.http import MediaFileUpload

import gdrive_authentication


class AutoIncrementer:
    def __init__(self):
        self.next = 0

    def get_next(self):
        next_num = self.next
        self.next += 1
        return next_num

    def reset(self):
        self.next = 0


app = Flask(__name__)
CORS(app)


@app.route("/")
def hello_world():
    return "<p>This is the home page of the website.</p>"


@app.route("/upload", methods=["POST"])
def recording_listener():
    recording = request.files["audio"]
    # question_id = request.form["questionId"]
    # user_id = request.form["userId"]
    question_id = "60d0b89eba9c14e2eef1b791"
    user_id = "60d0ade3ba9c14e2eef1b78e"

    file_path = save_recording(recording, {"questionId": question_id, "userId": user_id})
    upload_audio(file_path)
    # Notify the processor afterwards

    return render_template("submission.html")


@app.route("/answerquestion/", methods=["GET"])
def select_answer_question():
    if not rec_question_ids:
        return {"err": "rec_not_found"}
    next_question = rec_questions.find_one({"_id": random.choice(rec_question_ids)})
    audio_cursor = audio_descriptors.find(
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
    if not unrec_question_ids:
        return {"err": "unrec_not_found"}
    next_question_id = random.choice(unrec_question_ids)
    next_question = unrec_questions.find_one({"_id": next_question_id})
    result = next_question["transcript"]
    return {"transcript": result, "questionId": str(next_question_id)}


@app.route("/uploadtest/")  # Do not include in deployment.
def recording_listener_test():
    return render_template("uploadtest.html")


def save_recording(file, metadata):
    file_name = get_file_name()
    file_path = os.path.join(REC_DIR, file_name)
    file.save(file_path + ".wav")
    with open(file_path + ".json", "w") as meta_f:
        json.dump(metadata, meta_f)
    return file_path + ".wav"


def get_file_name():
    return str(queue_id_gen.get_next())


def upload_to_gdrive(file_path):
    if gdrive.creds.expired:
        gdrive.refresh()
    file_metadata = {"name": str(datetime.now()) + ".wav"}
    media = MediaFileUpload(file_path, mimetype="audio/wav")
    gfile = gdrive.drive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    os.remove(file_path)
    os.remove(file_path.split(".wav")[0] + ".json")
    gfile_id = gfile.get("id")

    permission = {"type": "anyone", "role": "reader"}
    gdrive.drive.permissions().create(fileId=gfile_id, body=permission, fields="id").execute()
    return gfile_id


def get_vtt(recording, transcript):
    return "placeholder vtt for \"" + transcript + "\""


def get_accuracy(transcript, recording):
    return 0.0


def get_ids(collection):
    ids = []
    id_cursor = collection.find(None, {"_id": 1})
    for i, doc in enumerate(id_cursor):
        ids.append(doc["_id"])
    id_cursor.close()  # Do I really need to do this?
    return ids


def upload_audio(file_path):
    with open(file_path.split(".wav")[0] + ".json") as meta_f:
        metadata = json.load(meta_f)
        question_id = ObjectId(metadata["questionId"])
        user_id = ObjectId(metadata["userId"])

    question_query = {"_id": question_id}
    unrecorded = True
    question = unrec_questions.find_one_and_delete(question_query)
    if question is None:
        unrecorded = False
        question = rec_questions.find_one(question_query)
    else:
        unrec_question_ids.remove(question_id)
    transcript = question["transcript"]

    with open(file_path) as recording:
        vtt = get_vtt(recording, transcript)
        accuracy = get_accuracy(transcript, recording)

    file_id = upload_to_gdrive(file_path)

    audio_id = file_id
    audio_entry = {"_id": file_id, "vtt": vtt, "version": VERSION, "user": user_id, "question": question_id,
                   "accuracy": accuracy}
    audio_descriptors.insert_one(audio_entry)

    if unrecorded:
        question["recordings"] = [audio_id]
        rec_questions.insert_one(question)
        rec_question_ids.append(question_id)
    else:
        rec_questions.update_one(question_query, {"$push": {"recordings": audio_id}})

    user_query = {"_id": user_id}
    users.update_one(user_query, {"$push": {"recordedAudios": audio_id}})


# TODO: Find out how to make these not global variables.
VERSION = "0.0.0"
SERVER_DIR = os.environ['SERVER_DIR']
SECRET_DATA_DIR = os.path.join(SERVER_DIR, "privatedata")
REC_DIR = os.path.join(SERVER_DIR, "recordings")

with open(os.path.join(SECRET_DATA_DIR, "connectionstring")) as f:
    client = pymongo.MongoClient(f.read())

database = client.QuizzrDatabase
users = database.Users
rec_questions = database.RecordedQuestions
unrec_questions = database.UnrecordedQuestions
audio_descriptors = database.Audio

gdrive = gdrive_authentication.GDriveAuth(SECRET_DATA_DIR)

rec_question_ids = get_ids(rec_questions)
unrec_question_ids = get_ids(unrec_questions)
queue_id_gen = AutoIncrementer()

if __name__ == "__main__":
    # recording_listener_test_desktop()
    pass
