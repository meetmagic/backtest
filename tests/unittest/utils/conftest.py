from pytest import fixture
from utils.env_setting import Settings
from utils.dbengine import BacktestDB


@fixture(scope="session", autouse=False)
def env():
    env = Settings("dev")
    return env


@fixture(scope="session", autouse=False)
def mongodb(env):
    return env.mongodb


@fixture(scope="session", autouse=False)
def backtestdb(env):
    backtestdb = BacktestDB(env.mongodb)
    return backtestdb