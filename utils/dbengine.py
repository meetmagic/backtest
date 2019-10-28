import pymongo
import json

from utils.env_setting import SETTINGS


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        if name == "MongoDBModel":
            return type.__new__(cls, name, bases, attrs)

        collection = attrs.get("__collection__", None) or name
        fields = attrs.get("__fields__", [])

        attrs["__collection__"] = collection
        attrs["__fields__"] = fields

        return type.__new__(cls, name, bases, attrs)


class MongoDatabase:

    def __init__(self):
        mongodb = SETTINGS.mongodb
        host = f'mongodb://{mongodb["username"]}:{mongodb["password"]}@{mongodb["client"]}/{mongodb["database"]}'
        self.client = pymongo.MongoClient(host=host)
        self.db = self.client.get_database()

    def find(self, table_name, **kwargs):
        return self.db[table_name].find(kwargs)

    def save_dataframe_to_collection(self, collection, dataframe):
        data_in_dict = dataframe.to_dict("records")
        result = self.db[collection].insert_many(data_in_dict)
        return result.inserted_ids

    def delete_and_save_dataframe_to_collection(self, collection, dataframe, query=None):
        query = {} if query is None else query

        result = self.db[collection].delete_many(query)

        if result.deleted_count is not None:
            data_in_dict = dataframe.to_dict("records")
            result = self.db[collection].insert_many(data_in_dict)
            return result.inserted_ids


class MongoDBModel(dict, metaclass=ModelMetaclass):

    def __init__(self, **kwargs):
        super(MongoDBModel, self).__init__(**kwargs)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f'{MongoDBModel} does not have attribute {key}')

    def __setattr__(self, key, value):
        self[key] = value

    def get_value(self, key):
        return getattr(self, key, None)

    def get_value_or_default(self, key):
        pass

    @classmethod
    def find(cls, **kwargs):
        client = MongoDBClient()
        documents = client.find(cls.__collection__, **kwargs)
        instances = []
        for document in documents:
            instances.append(cls(**document))

        return instances
