import pymongo


class BacktestDB:

    def __init__(self, mongodb):
        host = (f'mongodb://{mongodb["username"]}:{mongodb["password"]}@'
                f'{mongodb["client"]}')
        client = pymongo.MongoClient(host=host)
        self.db = client[mongodb["database"]]
