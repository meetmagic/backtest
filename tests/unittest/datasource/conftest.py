from pytest import fixture
from utils.env_setting import Settings
from datasource.from_tushare import TushareHanlder


@fixture(scope="session", autouse=False)
def settings():
    env = Settings("dev")
    return env


@fixture(scope="session", autouse=False)
def tusharehandler(settings):
    tusharehandler = TushareHanlder(settings.tushare["token"])
    return tusharehandler
