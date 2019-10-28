import tushare

from datetime import date
from time import time
from pymongo import ASCENDING

from utils.env_setting import SETTINGS
from utils.dbengine import MongoDatabase, MongoDBModel

EXCHANGES = {
    "SSE": "SHANGHAI STOCK EXCHANGE",
    "SZSE": "SHENZHEN STOCK EXCHANGE",
    # "HKEX": "HONG KONG EXCHANGE"
}


class TushareHanlder(MongoDatabase):

    def __init__(self, token=None):
        token = token if token else SETTINGS.tushare["token"]
        self.ts_pro = tushare.pro_api(token)
        super(TushareHanlder, self).__init__()

    def save_stock_basic(self):
        collection = "tushare_stock_basic"
        list_statuses = ["L", "D", "P"]
        for status in list_statuses:
            stock_basic = self.ts_pro.stock_basic(
                exchange="",
                list_status=status,
                fields="ts_code,symbol,name,area,industry,fullname, enname, "
                       "market, exchange, curr_type, list_status, list_date, "
                       "delist_date, is_hs"
            )
            query = {
                "list_status": status
            }
            self.delete_and_save_dataframe_to_collection(collection, stock_basic, query)

    def save_trading_calendar(self, year):
        collection = "tushare_trading_calendar"
        for exchange in EXCHANGES:
            trading_calendar = self.ts_pro.trade_cal(
                exchange=exchange,
                start_date=f'{year}0101',
                end_date=f'{year}1231',
                fields="exchange,cal_date,is_open,pretrade_date"
            )
            query = {
                "cal_date": {"$gte": f'{year}0101', "$lte": f'{year}1231'},
                "exchange": exchange
            }

            if not trading_calendar.empty:
                self.delete_and_save_dataframe_to_collection(collection, trading_calendar, query)

    def save_daily_bar(self, ts_code=None, trade_date=None):
        if (not ts_code) and (not trade_date):
            raise ValueError(f"Neither ts_code nor trade_date was provided.")

        collection = "tushare_daily_bar"

        if ts_code:
            stock_basic = self.get_stock_basic(ts_code)
            if stock_basic["list_status"] == "D":
                start_date = stock_basic["list_date"]
                end_date = stock_basic["delist_date"]
            elif stock_basic["list_status"] in ["L", "P"]:
                start_date = stock_basic["list_date"]
                end_date = self._last_trading_date(stock_basic["exchange"])
            else:
                raise ValueError(f'{ts_code} does not trade in SSE or SZSE.')
            query = {
                "ts_code": ts_code
            }
            stock_daily_bar = self.ts_pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
            if not stock_daily_bar.empty:
                size = stock_daily_bar.shape[0]
                while min(stock_daily_bar["trade_date"]) != start_date and size == 4000:
                    end_date = self._get_previous_trading_date(min(stock_daily_bar["trade_date"]), stock_basic["exchange"])
                    other = self.ts_pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                    stock_daily_bar = stock_daily_bar.append(other, ignore_index=True)
                    size = other.shape[0]
            else:
                return
        elif trade_date:
            query = {
                "trade_date": trade_date
            }
            stock_daily_bar = self.ts_pro.daily(trade_date=trade_date)

        self.delete_and_save_dataframe_to_collection(collection, stock_daily_bar, query)

    def save_weekly_bar(self):
        pass

    def save_monthly_bar(self):
        pass

    def save_adjust_factor(self):
        pass

    def _last_trading_date(self, exchange):
        collection = "tushare_trading_calendar"
        query = {
            "cal_date": date.today().strftime("%Y%m%d"),
            "exchange": exchange
        }
        today_in_trading_calendar = self.db[collection].find_one(query)
        last_trading_date = (today_in_trading_calendar["cal_date"] if today_in_trading_calendar["is_open"]
                             else today_in_trading_calendar["pretrade_date"])
        return last_trading_date

    def _get_previous_trading_date(self, trade_date, exchange):
        collection = "tushare_trading_calendar"
        query = {
            "cal_date": trade_date,
            "exchange": exchange
        }
        projection = {
            "pretrade_date": True
        }
        trade_date = self.db[collection].find_one(query, projection=projection)
        return trade_date["pretrade_date"]

    def get_stock_basic(self, ts_code=None, industry=None):
        collection = "tushare_stock_basic"
        query = {}
        if ts_code:
            query["ts_code"] = ts_code
        if industry:
            query["industry"] = industry

        return self.db[collection].find_one(query)

    def get_stocks_basic(self, ts_code=None, industry=None):
        collection = "tushare_stock_basic"
        query = {}
        if ts_code:
            query["ts_code"] = ts_code
        if industry:
            query["industry"] = industry

        return self.db[collection].find(query)

    def get_daily_bar(self, ts_code):
        collection = "tushare_daily_bar"
        query = {
            "ts_code": ts_code
        }
        sort = [("trade_date", ASCENDING)]
        daily_bar = self.db[collection].find(query).sort(sort)
        return daily_bar


class TushareDailyPrice(MongoDBModel):

    __collection__ = "tushare_daily_bar"
    __fields__ = ["ts_code", "trade_date", "open", "high", "low", "close", "change", "pct_chg", "vol", "amount"]


def tushare_daily_tasks(tushare_handler):
    tushare_handler.save_stock_basic()
    trade_date = date.today().strftime("%Y%m%d")
    # trade_date = '20191010'
    tushare_handler.save_daily_bar(trade_date=trade_date)
    # tushare_handler.save_daily_bar(trade_date="20190909")


def tushare_annual_tasks(tushare_handler):
    for year in range(1990, 2020):
        tushare_handler.save_trading_calendar(year)


def tushare_save_all_stocks_bar():
    tushare_token_mapping = {
        "magic_habit@hotmail.com": "f27c7447637fb48e933c66392d7544100b466819e36a7b1830a100ac",
        "magic_habit@live.com": "c6806eb344d04ba87419864b93ed1f249aec85666cf8d5e2be4797e1",
        "17204887@qq.com": "faedaa58caed37947c3c338ab2f2220edc9a1be8cce74d3d9f8fd111",
        "17836498@qq.com": "66766bf76f22b9cd0ce06cd0bfb0112ecb4390c7324446e694136bad",
        "14851085@qq.com": "cc571901f2407bbfe1a68b1e7a9ff15ff5c983d90ab1f297aeedb0f8"
    }

    tushare_handlers = [TushareHanlder(token) for token in tushare_token_mapping.values()]

    ts_codes = [ts_code["ts_code"] for ts_code in tushare_handlers[0].db.tushare_stock_basic.find({}, {"ts_code": True, "_id": False})]

    for index, ts_code in enumerate(ts_codes):
        start_time = time()
        tushare_handlers[(index % 5)].save_daily_bar(ts_code)
        elapsed_time = time() - start_time

        print(f"{index}th stock - {ts_code}: took {elapsed_time} seconds\n")
