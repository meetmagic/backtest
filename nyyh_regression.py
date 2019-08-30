from datasource.from_cninfo import CNInfoHandler
from datasource.from_tushare import TushareHanlder

if __name__ == "__main__":
    tushare_handler = TushareHanlder()
    cninfo_handler = CNInfoHandler()

    daily_bar = tushare_handler.get_daily_bar("601288")
    dividend_history = [individend for individend in cninfo_handler.get_dividend("601288")]

