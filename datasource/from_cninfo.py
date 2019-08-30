import pandas as pd
from pymongo import ASCENDING

from utils.dbengine import MongoDatabase


class CNInfoHandler(MongoDatabase):

    def __init__(self, token=None):
        self.base_url = "http://www.cninfo.com.cn"
        super(CNInfoHandler, self).__init__()

    def save_dividend(self, symbol, path="c:/gitrepo/backtest/data"):
        '''
        headers_mapping = {
            "公告日期": "announcement_date",
            "除权日": "ex_date",
            "股权登记日": "record_date",
            "每股现金股利(元)": "dividend",
            "每股送红股": "bonus_share",
            "每股转增股份": "extended_share",
            "现金到账日": "dividend_payment_date",
            "股份到账日": "share_payment_date"
        }
        :param symbol:
        :param path:
        :return:
        '''
        columns = ["announcement_date", "ex_date", "record_date", "dividend", "bonus_share",
                   "extended_share", "dividend_payment_date", "share_payment_date"]

        def conversion(source):
            return str(source).replace("/", "")
        column_converter_mapping = {
            0: conversion,
            1: conversion,
            2: conversion,
            6: conversion,
            8: conversion
        }

        dividend = pd.read_csv(f'{path}/{symbol}_dividend.csv', names=columns, converters=column_converter_mapping)
        dividend.drop([0], inplace=True)
        for col in ["dividend", "bonus_share", "extended_share"]:
            dividend[col] = pd.to_numeric(dividend[col])
        dividend["symbol"] = symbol
        collection = "cninfo_dividend"
        query = {
            "symbol": symbol
        }
        self.delete_and_save_dataframe_to_collection(collection, dividend, query)

    def get_dividend(self, symbol):
        collection = "cninfo_dividend"
        query = {
            "symbol": symbol
        }
        sort = [("ex-date", ASCENDING)]
        dividend_history = self.db[collection].find(query).sort(sort)
        return dividend_history