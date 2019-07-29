import pymongo

MONGODB_CLIENT = {
    "ip_address": "127.0.0.1",
    "port": 27017
}
MONGODB_NAME = "backtest"
MONGODB_USER = {
    "user_name": "backtest",
    "password": "testback"
}


class BacktestDB:

    def __init__(self):
        host = (f'mongodb://{MONGODB_USER["user_name"]}:{MONGODB_USER["password"]}@'
                f'{MONGODB_CLIENT["ip_address"]}:{MONGODB_CLIENT["port"]}')
        client = pymongo.MongoClient(host=host)
        self.db = client[MONGODB_NAME]
