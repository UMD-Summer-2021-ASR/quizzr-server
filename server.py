# from typing import Dict, List, Union
import os
import random

from bson import ObjectId
from flask import Flask, request, render_template
import pymongo
from googleapiclient.http import MediaFileUpload

import gdrive_authentication


"""users = {"johnDoe": {"recordedAudios": []}}
rec_questions = {
    "question1": {
        "transcript": "The quick brown fox jumps over the lazy dog.", "recordings": ["audio1"]
    }
}
# unrec_questions: Dict[str, Dict[str, Union[str, List[str]]]] = {
unrec_questions = {
    "question2": {
        "transcript": "Hello world."
    }
}
audio_descriptors = {
    "audio1": {
        "fileId": "Placeholder file ID 1",
        "vtt": "Placeholder vtt data 1",
        "version": "0.0.0",
        "accuracy": 0.0,
        "user": "johnDoe",
        "question": "question1"
    }
}"""

app = Flask(__name__)


@app.route("/")
def hello_world():
    return "<p>This is the home page of the website.</p>"


@app.route("/upload/", methods=["POST"])
def recording_listener():
    recording = request.files["audio"]
    # question_id = request.form["questionId"]
    # user_id = request.form["userId"]
    question_id = ObjectId(request.form["questionId"])
    user_id = ObjectId(request.form["userId"])

    question_query = {"_id": question_id}
    unrecorded = True
    question = unrec_questions.find_one_and_delete(question_query)
    if question is None:
        unrecorded = False
        question = rec_questions.find_one(question_query)
    else:
        unrec_question_ids.remove(question_id)

    transcript = question["transcript"]
    file_id = upload_file(recording)
    vtt = get_vtt(recording, transcript)

    audio_id = file_id
    audio_entry = {"_id": file_id, "vtt": vtt, "version": VERSION, "user": user_id, "question": question_id,
                   "accuracy": get_accuracy(transcript, recording)}
    audio_descriptors.insert_one(audio_entry)

    if unrecorded:
        question["recordings"] = [audio_id]
        rec_questions.insert_one(question)
        rec_question_ids.append(question_id)
    else:
        rec_questions.update_one(question_query, {"$push": {"recordings": audio_id}})

    user_query = {"_id": user_id}
    users.update_one(user_query, {"$push": {"recordedAudios": audio_id}})

    return render_template("submission.html")


@app.route("/answerquestion/", methods=["GET"])
def select_answer_question():
    if not rec_question_ids:
        result = "rec_not_found"
        return "Could not find a question to answer."
    next_question = rec_questions.find_one({"_id": random.choice(rec_question_ids)})
    audio_cursor = audio_descriptors.find(
        {"_id": {"$in": next_question["recordings"]}},
        {"_id": 1, "vtt": 1, "accuracy": 1}
    )
    audio_cursor.sort("accuracy", pymongo.DESCENDING)
    audio = audio_cursor[0]
    audio_cursor.close()
    result = {"vtt": audio["vtt"], "fileId": audio["_id"]}
    return f'VTT: {result["vtt"]}, File ID: {result["fileId"]}'


@app.route("/recordquestion/", methods=["GET"])
def select_record_question():
    if not unrec_question_ids:
        return "Could not find a question to record."
    next_question = unrec_questions.find_one({"_id": random.choice(unrec_question_ids)})
    result = next_question["transcript"]
    return f'Transcript: {result}'


@app.route("/uploadtest/")  # Do not include in deployment.
def recording_listener_test():
    return render_template("uploadtest.html")


def upload_file(file):
    # TODO: Determine if it is possible to do this in a way that does not require storing the file on the machine.
    file_name = "temp.wav"
    file_path = os.path.join(REC_DIR, file_name)
    file.save(file_path)
    file_metadata = {}
    media = MediaFileUpload(file_path, mimetype="audio/wav")
    gfile = gdrive.files().create(body=file_metadata, media_body=media, fields="id").execute()
    os.remove(file_path)
    return gfile.get("id")


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

gdrive = gdrive_authentication.setup_drive(SECRET_DATA_DIR)  # TODO: Use gdrive_authentication.GDriveAuth() instead

rec_question_ids = get_ids(rec_questions)
unrec_question_ids = get_ids(unrec_questions)

if __name__ == "__main__":
    # recording_listener_test_desktop()
    pass
