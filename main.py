import pandas as pd

from datasource.from_tushare import tushare_daily_tasks, TushareHanlder, tushare_annual_tasks, tushare_save_all_stocks_bar
from datasource.from_cninfo import CNInfoHandler

if __name__ == "__main__":
    tushare_handler = TushareHanlder()
    #
    # tushare_daily_tasks(tushare_handler)
    # tushare_annual_tasks(tushare_handler)

    # cninfo_handler = CNInfoHandler()
    # cninfo_handler.save_dividend("601288")
    nyyh = tushare_handler.get_daily_bar("601228.SH")
    pd.DataFrame(nyyh).to_csv("c:/gitrepo/backtest/data/nyyh_daily_bar.csv")
