from pandas import DataFrame

class TestFromTushare:

    def test_tushare_initialization(self, tusharehandler):
        stocks = tusharehandler.ts_pro.stock_basic(exchange='', list_status='D')
        assert isinstance(stocks, DataFrame), "Did not initialize tushare api correctly"


