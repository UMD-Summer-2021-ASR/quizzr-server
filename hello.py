# from typing import Dict, List, Union

from flask import Flask, request
# from flask import request

app = Flask(__name__)

# TEMPORARY.
version = "0.0.0"
users = {"johnDoe": {"recordedAudios": []}}
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
}

@app.route("/")
def hello_world():
    return "<p>Hello, World!</p>"


@app.route("/upload/", methods=["POST"])
def recording_listener():
    if request.method == "POST":
        recording = request.form["audio"]
        question_id = request.form["questionId"]
        user_id = request.form["userId"]

        try:
            transcript = unrec_questions[question_id]
            unrecorded = True
        except KeyError:
            transcript = rec_questions[question_id]
            unrecorded = False

        file_id = upload_file(recording)
        vtt = get_vtt(recording, transcript)

        audio_id = generate_audio_id()
        audio_entry = {"fileId": file_id, "vtt": vtt, "version": version, "user": user_id, "question": question_id,
                       "accuracy": get_accuracy(transcript, recording)}
        # TODO: Put the set of instructions below in MongoDB, appropriately translated.
        audio_descriptors[audio_id] = audio_entry
        if unrecorded:
            temp_question = unrec_questions.pop(question_id, None)
            temp_question["recordings"] = [audio_id]
            unrec_questions[question_id] = {}
            rec_questions[question_id] = temp_question
        else:
            rec_questions[question_id]["recordings"].append(audio_id)
        users[user_id]["recordedAudios"].append(audio_id)


def recording_listener_test():
    print("Users: ", users)
    print("Recorded Questions: ", rec_questions)
    print("Unrecorded Questions: ", unrec_questions)
    print("Audio Descriptors: ", audio_descriptors)
    print()

    # recording = open(input("Enter a directory for an audio file: "))
    recording = ""
    question_id = "question2"
    user_id = "johnDoe"

    try:
        transcript = unrec_questions[question_id]["transcript"]
        unrecorded = True
    except KeyError:
        transcript = rec_questions[question_id]["transcript"]
        unrecorded = False

    file_id = upload_file(recording)
    vtt = get_vtt(recording, transcript)

    audio_id = generate_audio_id()
    audio_entry = {"fileId": file_id, "vtt": vtt, "version": version, "user": user_id, "question": question_id,
                   "accuracy": get_accuracy(transcript, recording)}
    # TODO: Put the set of instructions below in MongoDB, appropriately translated.
    audio_descriptors[audio_id] = audio_entry
    if unrecorded:
        temp_question = unrec_questions.pop(question_id, None)
        temp_question["recordings"] = [audio_id]
        unrec_questions[question_id] = {}
        rec_questions[question_id] = temp_question
    else:
        rec_questions[question_id]["recordings"].append(audio_id)
    users[user_id]["recordedAudios"].append(audio_id)

    print("Users: ", users)
    print("Recorded Questions: ", rec_questions)
    print("Unrecorded Questions: ", unrec_questions)
    print("Audio Descriptors: ", audio_descriptors)


def upload_file(file):
    return "placeholder file ID 2"


def get_vtt(audio, transcript):
    return "placeholder vtt for \"" + transcript + "\""


def generate_audio_id():
    return "placeholder audio ID 2"


def get_accuracy(transcript, recording):
    return 0.0


if __name__ == "__main__":
    recording_listener_test()
