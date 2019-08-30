from pytest import fixture

from datasource.from_tushare import TushareHanlder, TushareDailyPrice
from datasource.from_cninfo import CNInfoHandler


@fixture(scope="session", autouse=False)
def tusharehandler():
    tusharehandler = TushareHanlder()
    return tusharehandler


@fixture(scope="session", autouse=False)
def tusharedailyprice():
    tusharedailyprice = TushareDailyPrice.find(ts_code="601288.SH")
    return tusharedailyprice

@fixture(scope="session", autouse=False)
def cninfohandler():
    cninfohandler = CNInfoHandler()
    return cninfohandler