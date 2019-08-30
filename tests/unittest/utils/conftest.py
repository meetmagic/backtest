from pytest import fixture

from utils.env_setting import SETTINGS
from utils.dbengine import MongoDBClient


@fixture(scope="session", autouse=False)
def mongodb():
    return SETTINGS.mongodb


@fixture(scope="session", autouse=False)
def mongodbclient():
    return MongoDBClient()
