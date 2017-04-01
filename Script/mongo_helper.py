# coding=utf-8
from pymongo import MongoClient

__client = MongoClient()


def get_coll(collection, db):
    # get collection instance
    database = __client[db]
    coll = database[collection]
    return coll


def write_mongo(document, coll):
    # write document(type: dict) to collection coll
    coll.insert_one(document)


def close_mongo():
    __client.close()
