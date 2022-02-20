import os

import pymongo
from pymongo import UpdateMany


def main():
    """
    Rename the "gentleVtt" field of every audio document to "vtt" and the "vtt" field of every audio document to
    "oldVtt" (if it has a "gentleVtt" field).

    Environment variables:

    * ``DATABASE`` - The name of the database to rename the VTT fields in
    """
    db_name = os.environ.get("DATABASE")
    if not db_name:
        raise ValueError("Environment variable 'DATABASE' not defined")

    mongodb = pymongo.MongoClient(os.environ["CONNECTION_STRING"])
    audio_coll = mongodb.get_database(db_name).get_collection("Audio")
    unproc_audio_coll = mongodb.get_database(db_name).get_collection("UnprocessedAudio")
    update_batch = [
        UpdateMany({"vtt": {"$exists": True}, "gentleVtt": {"$exists": True}}, {"$rename": {"vtt": "oldVtt"}}),
        UpdateMany({"gentleVtt": {"$exists": True}}, {"$rename": {"gentleVtt": "vtt"}})
    ]
    audio_coll.bulk_write(update_batch)
    unproc_audio_coll.bulk_write(update_batch)


if __name__ == '__main__':
    main()
