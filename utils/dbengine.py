import pymongo
from .env_setting import Settings

class BacktestDB:

    def __init__(self, env=None):
        mongodb = Settings(env=env).mongodb
        host = (f'mongodb://{mongodb["username"]}:{mongodb["password"]}@'
                f'{mongodb["client"]}')
        client = pymongo.MongoClient(host=host)
        self.db = client[mongodb["database"]]
