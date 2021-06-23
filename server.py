# from typing import Dict, List, Union
import os
from os import environ
from os.path import join

from bson import ObjectId
from flask import Flask, request, render_template
import pymongo
from googleapiclient.http import MediaFileUpload

import gdrive_authentication

version = "0.0.0"
SERVER_DIR = environ['SERVER_DIR']
SECRET_DATA_DIR = join(SERVER_DIR, "privatedata")
REC_DIR = join(SERVER_DIR, "recordings")

with open(join(SECRET_DATA_DIR, "connectionstring")) as f:
    client = pymongo.MongoClient(f.read())

database = client.QuizzrDatabase
users = database.Users
rec_questions = database.RecordedQuestions
unrec_questions = database.UnrecordedQuestions
audio_descriptors = database.Audio

gdrive = gdrive_authentication.setup_drive(SECRET_DATA_DIR)

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
    if request.method == "POST":
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

        transcript = question["transcript"]
        file_id = upload_file(recording)
        vtt = get_vtt(recording, transcript)

        audio_id = file_id
        audio_entry = {"_id": file_id, "vtt": vtt, "version": version, "user": user_id, "question": question_id,
                       "accuracy": get_accuracy(transcript, recording)}

        audio_descriptors.insert_one(audio_entry)

        if unrecorded:
            question["recordings"] = [audio_id]
            rec_questions.insert_one(question)
        else:
            rec_questions.update_one(question_query, {"$push": {"recordings": audio_id}})

        user_query = {"_id": user_id}
        user = users.find_one(user_query)
        user["recordedAudios"].append(audio_id)
        users.update_one(user_query, {"$push": {"recordedAudios": audio_id}})

        return render_template("submission.html")

    return "<p>Hello, World!</p>"


# route: answerquestion, func: answer_question
# route: recordquestion, func: record_question
@app.route("/answerquestion/")
def select_answer_question():
    pass


@app.route("/recordquestion/")
def select_record_question():
    pass


@app.route("/uploadtest/")  # Do not include in deployment.
def recording_listener_test():
    return render_template("uploadtest.html")


def upload_file(file):
    # TODO: Determine if it is possible to do this in a way that does not require storing the file on the machine.
    file_name = "temp.wav"
    file_path = join(REC_DIR, file_name)
    file.save(file_path)
    file_metadata = {}
    media = MediaFileUpload(file_path, mimetype='audio/wav')
    gfile = gdrive.files().create(body=file_metadata, media_body=media, fields='id').execute()
    os.remove(file_path)
    return gfile.get('id')


def get_vtt(audio, transcript):
    return "placeholder vtt for \"" + transcript + "\""


def generate_audio_id():
    return "placeholder audio ID 2"


def get_accuracy(transcript, recording):
    return 0.0


if __name__ == "__main__":
    # recording_listener_test_desktop()
    pass
