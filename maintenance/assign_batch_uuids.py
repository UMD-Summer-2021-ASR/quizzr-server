import os
from uuid import uuid4

import pymongo
from pymongo import UpdateOne
from pymongo.collection import Collection


def main():
    """
    Retroactively add batch UUIDs to every audio document containing sentence or tokenization IDs in the given database.

    Note: The assignment of these batch UUIDs is not perfect and cannot differentiate between recording sessions for the
    same user and question.

    Environment variables:

    * ``DATABASE`` - The name of the database to add batch UUIDs in
    """
    db_name = os.environ.get("DATABASE")
    if not db_name:
        raise ValueError("Environment variable 'DATABASE' not defined")

    mongodb = pymongo.MongoClient(os.environ["CONNECTION_STRING"])
    audio_coll = mongodb.get_database(db_name).get_collection("Audio")
    unproc_audio_coll = mongodb.get_database(db_name).get_collection("UnprocessedAudio")

    def assign_uuids(cltn: Collection, query):
        """Anti-code-duplication measures"""
        update_batch = []
        batch_context = {}
        cursor = cltn.find(query)
        for rec in cursor:
            for batch_uuid, context in batch_context.items():
                # Reuse a batch UUID
                # Conditions: If the recording has a tokenization ID, the batch context must contain a list of seen
                # tokenization IDs, and its tokenization ID must not be part of that list. If the recording has a
                # sentence ID, the batch context must contain a list of seen sentence IDs, and its sentence ID must not
                # be part of that list.
                sid_cond = ("sentenceId" in rec
                            and "seenSentenceIds" in context
                            and rec["sentenceId"] not in context["seenSentenceIds"])
                token_id_cond = ("tokenizationId" in rec
                                 and "seenTokenizationIds" in context
                                 and rec["tokenizationId"] not in context["seenTokenizationIds"])

                if (rec["qb_id"] == context["qb_id"] and rec["userId"] == context["userId"]
                        and (token_id_cond or sid_cond)):
                    update_batch.append(UpdateOne({"_id": rec["_id"]}, {"$set": {"batchUUID": batch_uuid}}))

                    if sid_cond:
                        context["seenSentenceIds"].append(rec["sentenceId"])
                    elif token_id_cond:
                        context["seenTokenizationIds"].append(rec["tokenizationId"])
                    break
            else:
                # Make a new batch ID
                next_batch_uuid = str(uuid4())
                update_batch.append(UpdateOne({"_id": rec["_id"]}, {"$set": {"batchUUID": next_batch_uuid}}))
                if "sentenceId" in rec:
                    batch_context[next_batch_uuid] = {
                        "seenSentenceIds": [rec["sentenceId"]],
                        "qb_id": rec["qb_id"],
                        "userId": rec["userId"]
                    }
                else:
                    batch_context[next_batch_uuid] = {
                        "seenTokenizationIds": [rec["tokenizationId"]],
                        "qb_id": rec["qb_id"],
                        "userId": rec["userId"]
                    }

        if update_batch:
            cltn.bulk_write(update_batch)

    query = {
        "$or": [
            {"sentenceId": {"$exists": True}},
            {"tokenizationId": {"$exists": True}}
        ],
        "batchUUID": {"$exists": False}
    }
    assign_uuids(audio_coll, query)
    assign_uuids(unproc_audio_coll, query)


if __name__ == '__main__':
    main()
