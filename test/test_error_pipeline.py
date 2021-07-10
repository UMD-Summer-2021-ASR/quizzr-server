import logging
from http import HTTPStatus

import bson
import pytest

from corruption import shatter_dict
from testutil import generate_audio_id, match_status


@pytest.mark.usefixtures("client")
class TestUnprocAudio:
    @pytest.fixture
    def route(self):
        return "/audio/unprocessed/"

    @pytest.fixture
    def unrec_questions(self, quizzr_server):
        question_docs = [
            {},
            {"transcript": "Foo"}
        ]
        results = quizzr_server.insert_unrec_questions(question_docs)
        yield results.inserted_ids
        quizzr_server.delete_unrec_questions({"_id": {"$in": results.inserted_ids}})

    @pytest.fixture
    def audio_doc_no_qid(self, quizzr_server):
        result = quizzr_server.unproc_audio.insert_one({"_id": generate_audio_id()})
        yield result.inserted_id
        quizzr_server.unproc_audio.delete_one({"_id": result.inserted_id})

    @pytest.fixture
    def audio_doc_invalid_qid(self, quizzr_server):
        result = quizzr_server.unproc_audio.insert_one({"_id": generate_audio_id(), "questionId": bson.ObjectId()})
        yield result.inserted_id
        quizzr_server.unproc_audio.delete_one({"_id": result.inserted_id})

    @pytest.fixture
    def audio_docs(self, quizzr_server, unrec_questions):
        entries = []
        for qid in unrec_questions:
            entries.append({"_id": generate_audio_id(), "questionId": qid})
        audio_results = quizzr_server.unproc_audio.insert_many(entries)
        yield
        quizzr_server.unproc_audio.delete_many({"_id": {"$in": audio_results.inserted_ids}})

    def test_empty(self, client, route):
        test_values = {
            "status": HTTPStatus.NOT_FOUND,
            "resp": b'empty_unproc_audio'
        }
        result = client.get(route)
        assert match_status(test_values["status"], result.status)
        assert result.get_data() == test_values["resp"]

    def test_no_qid(self, client, route, audio_doc_no_qid):
        test_values = {
            "status": HTTPStatus.NOT_FOUND,
            "resp": b'empty_qid2entries'
        }
        response = client.get(route)
        assert match_status(test_values["status"], response.status)
        assert response.get_data() == test_values["resp"]

    def test_invalid_qid(self, client, route, audio_doc_invalid_qid):
        test_values = {
            "status": HTTPStatus.OK
        }
        response = client.get(route)
        assert match_status(test_values["status"], response.status)

    def test_corrupt(self, client, route, audio_docs, audio_doc_no_qid, audio_doc_invalid_qid, unrec_questions):
        test_values = {
            "status": HTTPStatus.OK,
            "errors": [
                {"type": "internal_error", "reason": "undefined_question_id"}
            ]
        }
        response = client.get(route)
        response_body = response.get_json()
        assert match_status(test_values["status"], response.status)
        for error in test_values["errors"]:
            assert error in response_body["errors"]


@pytest.mark.usefixtures("client", "quizzr_server")
class TestProcAudio:
    @pytest.fixture
    def route(self):
        return "/audio/processed"

    @pytest.fixture
    def unrec_question(self, quizzr_server):
        question_doc = {"transcript": "Foo"}
        result = quizzr_server.insert_unrec_question(question_doc)
        question_doc["_id"] = result.inserted_id
        yield question_doc
        quizzr_server.delete_unrec_question({"_id": result.inserted_id})
        quizzr_server.delete_rec_question({"_id": result.inserted_id})

    @pytest.fixture
    def unproc_audio_documents(self, quizzr_server, unrec_question):
        audio_docs = [
            {},
            {"questionId": bson.ObjectId()},
            {"questionId": bson.ObjectId(), "userId": bson.ObjectId()},
            {"questionId": unrec_question["_id"]},
            {"questionId": unrec_question["_id"], "userId": bson.ObjectId()}
        ]
        for doc in audio_docs:
            doc["_id"] = generate_audio_id()

        audio_results = quizzr_server.unproc_audio.insert_many(audio_docs)
        yield audio_results.inserted_ids
        quizzr_server.unproc_audio.delete_many({"_id": {"$in": audio_results.inserted_ids}})

    @pytest.fixture
    def update_batch(self, unproc_audio_documents, quizzr_server):
        batch = [{}, {"_id": generate_audio_id()}]
        id_list = []
        for doc_id in unproc_audio_documents:
            batch.append({"_id": doc_id, "vtt": "Placeholder"})
            id_list.append(doc_id)
        yield batch
        quizzr_server.audio.delete_many({"_id": {"$in": id_list}})

    def test_corrupt(self, client, route, update_batch):
        test_values = {
            "errors": [
                {"type": "bad_args", "reason": "undefined_gfile_id"},
                {"type": "bad_args", "reason": "invalid_gfile_id"},
                {"type": "internal_error", "reason": "undefined_question_id"},
                {"type": "internal_error", "reason": "question_update_failure"},
                {"type": "internal_error", "reason": "undefined_user_id"},
                {"type": "internal_error", "reason": "user_update_failure"}
            ]
        }
        response = client.post(route, json={"arguments": update_batch})
        response_body = response.get_json()
        for error in test_values["errors"]:
            assert error in response_body["errors"]


@pytest.mark.usefixtures("client", "quizzr_server")
class TestAnswer:
    @pytest.fixture
    def route(self):
        return "/answer/"

    @pytest.fixture
    def all_corrupt(self, quizzr_server):
        backup_qids = quizzr_server.rec_question_ids.copy()
        quizzr_server.rec_question_ids.append(bson.ObjectId())  # Invalid qid
        base_audio_doc = {
            "vtt": "The quick brown fox jumps over the lazy dog."
        }
        audio_docs = [doc for doc in shatter_dict(base_audio_doc, depth=-1)]
        for doc in audio_docs:
            doc["_id"] = generate_audio_id()
            doc["version"] = quizzr_server.meta["version"],
            doc["score"] = {"wer": 1.0, "mer": 1.0, "wil": 1.0}
        audio_results = quizzr_server.audio.insert_many(audio_docs)
        questions = [
            {}, {"recordings": []}, {"recordings": [generate_audio_id()]}, {"recordings": audio_results.inserted_ids}
        ]
        question_results = quizzr_server.insert_rec_questions(questions)
        # quizzr_server.rec_question_ids += question_results.inserted_ids
        yield
        quizzr_server.audio.delete_many({"_id": {"$in": audio_results.inserted_ids}})
        quizzr_server.rec_questions.delete_many({"_id": {"$in": question_results.inserted_ids}})
        quizzr_server.rec_question_ids = backup_qids

    @pytest.fixture
    def empty(self, quizzr_server):
        backup_qids = quizzr_server.rec_question_ids
        quizzr_server.rec_question_ids = []
        yield
        quizzr_server.rec_question_ids = backup_qids

    def test_all_corrupt(self, client, all_corrupt, caplog, route):
        test_values = {
            "logs": [
                " is invalid or associated question has no valid audio recordings",
                "No audio documents found",
                "Failed to find a viable audio document",
                "Audio document does not have VTT"
            ],
            "resp": b'rec_corrupt_questions',
            "status": HTTPStatus.INTERNAL_SERVER_ERROR
        }
        caplog.set_level(logging.DEBUG)
        response = client.get(route)
        assert match_status(test_values["status"], response.status)
        assert response.get_data() == test_values["resp"]
        for log in test_values["logs"]:
            assert log in caplog.text

    def test_empty(self, client, empty, route):
        test_values = {
            "resp": b'rec_empty_qids',
            "status": HTTPStatus.INTERNAL_SERVER_ERROR
        }
        response = client.get(route)
        assert match_status(test_values["status"], response.status)
        assert response.get_data() == test_values["resp"]


@pytest.mark.usefixtures("client", "quizzr_server")
class TestRecord:
    @pytest.fixture
    def route(self):
        return "/record/"

    @pytest.fixture
    def all_corrupt(self, quizzr_server):
        backup_qids = quizzr_server.unrec_question_ids.copy()
        quizzr_server.unrec_question_ids.append(bson.ObjectId())
        question_result = quizzr_server.insert_unrec_question({})
        yield
        quizzr_server.unrec_questions.delete_one({"_id": question_result.inserted_id})
        quizzr_server.unrec_question_ids = backup_qids

    @pytest.fixture
    def empty(self, quizzr_server):
        backup_qids = quizzr_server.unrec_question_ids
        quizzr_server.unrec_question_ids = []
        yield
        quizzr_server.unrec_question_ids = backup_qids

    def test_all_corrupt(self, client, route, all_corrupt, caplog):
        test_values = {
            "logs": [
                " is invalid or associated question has no transcript"
            ],
            "resp": b'unrec_corrupt_questions'
        }
        response = client.get(route)
        assert response.get_data() == test_values["resp"]
        for log in test_values["logs"]:
            assert log in caplog.text

    def test_empty(self, client, route, empty):
        test_values = {
            "resp": b'unrec_empty_qids',
            "status": HTTPStatus.INTERNAL_SERVER_ERROR
        }
        response = client.get(route)
        assert match_status(test_values["status"], response.status)
        assert response.get_data() == test_values["resp"]


def test_err_upload_args(client):
    audio = open("input/test.wav")

    client.post("/upload", data={
        "audio": (audio, ),
        "qid": "60e221e891e99d1d0ad61d65"
    })
