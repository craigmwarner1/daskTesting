from os import environ as e
from pymongo import MongoClient

class Database:
    def __init__(self):
        host = e['MONGO_HOST'] if 'MONGO_HOST' in e else 'localhost'
        port = e['MONGO_PORT'] if 'MONGO_PORT' in e else '27017'
        dbid = e['MONGO_DBID'] if 'MONGO_DBID' in e else 'TEST_DB'

        self.client = MongoClient(host, int(port))
        self.database = self.client[dbid]

    def read(self, collection, lookup = {}):
        return self.database[collection].find_one(lookup)

    def write(self, collection, contents):
        self.database[collection].insert_one(contents)
