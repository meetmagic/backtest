from pandas import DataFrame
from datasource.from_tushare import tushare_save_all_stocks_bar

class TestFromTushare:

    def test_tushare_initialization(self, tusharehandler):
        stocks = tusharehandler.ts_pro.stock_basic(exchange='', list_status='D')
        assert isinstance(stocks, DataFrame), "Did not initialize tushare api correctly"

    def test_save_stock_basic(self, tusharehandler):
        tusharehandler.save_stock_basic()
        stocks = tusharehandler.db.tushare_stock_basic.find({"symbol": "600018"})
        for stock in stocks:
            assert stock["symbol"] == "000001"
            assert stock["name"] == "平安银行"

    def test_save_trading_calendar(self, tusharehandler):
        tusharehandler.save_trading_calendar(2019)
        query = {
            "cal_date": "20190101"
        }
        dates = tusharehandler.db.tushare_trading_calendar.find(query)
        for trading_date in dates:
            assert trading_date["exchange"] in ["SSE", "SZSE"]
            assert trading_date["cal_date"] == "20190101"
            assert trading_date["is_open"] == 0

    def test_save_daily_bar_by_ts_code(self, tusharehandler):
        ts_code = "601288.SH"
        tusharehandler.save_daily_bar(ts_code=ts_code)
        query = {
            "ts_code": ts_code,
            "trade_date": "20110211",
        }
        collection = "tushare_daily_bar"
        daily_bars = tusharehandler.db[collection].find(query)
        for bar in daily_bars:
            assert bar["open"] == 2.61
            assert bar["high"] == 2.62
            assert bar["low"] == 2.6
            assert bar["close"] == 2.61

    def test_save_daily_bar_with_more_than_400_records(self, tusharehandler):
        ts_code = "000004.SZ"
        tusharehandler.save_daily_bar(ts_code=ts_code)
        collection = "tushare_daily_bar"
        query = {
            "ts_code": ts_code
        }
        count = tusharehandler.db[collection].find(query).count()
        assert count == 6585, f'the bar of {ts_code} is less than 6585.'

    def test_save_daily_bar_with_pending_stock(self, tusharehandler):
        stocks = tusharehandler.ts_pro.stock_basic(exchange='', list_status='P')
        if not stocks.empty:
            ts_code = stocks.iloc[0].ts_code
            tusharehandler.save_daily_bar(ts_code)
            collection = "tushare_daily_bar"
            query = {
                "ts_code": ts_code
            }
            count = tusharehandler.db[collection].find(query).count()
            assert count != 0, f'The bar of the pending stock {ts_code} was not saved.'

    def test_save_daily_bar_by_trade_date(self, tusharehandler):
        tusharehandler.save_daily_bar(trade_date="20190829")
        collection = "tushare_daily_bar"
        query = {
            "ts_code": "000001.SZ",
            "trade_date": "20190829"
        }
        result = tusharehandler.db[collection].find_one(query)
        assert result["open"] == 14.22
        assert result["high"] == 14.24
        assert result["low"] == 14.08
        assert result["close"] == 14.13


class TestTushareDailyPrice:
    def test_tushare_daily_price_initialization(self, tusharedailyprice):
        count = tusharedailyprice.count()
        print(count)
