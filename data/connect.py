from pymongo import MongoClient
import pandas as pd


class MongoDBConnection:
    def __init__(self, host='localhost', port=27017, database='Test_2'):
        self.host = host
        self.port = port
        self.database = database
        self.client = None
        self.db = None

    def __enter__(self):
        self.client = MongoClient(self.host, self.port)
        self.db = self.client[self.database]
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.client.close()

    def connect_collection(self, collection_name: str):
        collection = self.db[collection_name]
        return collection
    
    def get_collection(self, collection_name: str, condition: dict = {},
                       filter_collection: dict = {'_id': 0}):
        collection = self.db[collection_name]
        cursor = collection.find(condition, filter_collection)
        return list(cursor)

    def fill_data(self, collection_name: str, data: dict):
        collection = self.db[collection_name]
        collection.insert_one(data)

    def update_data(self, collection_name: str, condition: dict, new_data: dict):
        collection = self.db[collection_name]
        collection.update_one(condition, {'$set': new_data})

    def delete_data(self, collection_name: str, condition: dict):
        collection = self.db[collection_name]
        collection.delete_one(condition)

    def find(self, collection_name: str, condition: dict):
        collection = self.db[collection_name]
        cursor = collection.find(condition)
        return list(cursor)

    def find_one(self, collection_name: str, condition: dict):
        collection = self.db[collection_name]
        return collection.find_one(condition)
    
    def replace_one(self, collection_name: str, condition: dict, new_data: dict):
        collection = self.db[collection_name]
        collection.replace_one(condition, new_data)
    
    def aggregate(self, collection_name: str, pipeline: list):
        collection = self.db[collection_name]
        return collection.aggregate(pipeline)

    