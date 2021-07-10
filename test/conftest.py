import os

import pymongo
import pytest

import server
from server import create_app


@pytest.fixture(scope="session")
def client():
    app = create_app()
    with app.test_client() as client:
        return client


@pytest.fixture(scope="session")
def mongodb_client():
    connection_string = os.environ["CONNECTION_STRING"]
    return pymongo.MongoClient(connection_string)


@pytest.fixture
def quizzr_server():
    return server.qs


# def mongodb(mongodb_client):
@pytest.fixture(scope="session")
def mongodb(mongodb_client, quizzr_server):
    # database = mongodb_client.QuizzrDatabaseTest
    database = quizzr_server.database
    yield database
    query = {"_id": {"$exists": True}}
    database.Audio.delete_many(query)
    database.RecordedQuestions.delete_many(query)
    database.UnrecordedQuestions.delete_many(query)
    database.UnprocessedAudio.delete_many(query)
    database.Users.delete_many(query)
