import pandas as pd
import requests
import time
import csv
from datetime import date
from pymongo import ASCENDING

from utils.dbengine import MongoDatabase
from utils.env_setting import SETTINGS


class CNInfoHandler(MongoDatabase):

    def __init__(self, token=None):
        self.base_url = "http://www.cninfo.com.cn"
        # self.base_url = "http://webapi.cninfo.com.cn/api"
        super(CNInfoHandler, self).__init__()

    def save_dividend(self, symbol):
        field_mapping = {
            "F013D": "announcement_date",
            "F014D": "ex_date",
            "F015D": "record_date",
            "F010N": "dividend",
            "F011N": "bonus_share",
            "F012N": "extended_share",
            "F016D": "dividend_payment_date",
            "F017D": "share_payment_date"
        }

        # path = f'{self.base_url}/data/project/commonInterface'
        path = f'{self.base_url}/sysapi/p_sysapi1019?scode={symbol}&syear=2004'
        headers = {
            "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"
        }
        data = {
            "mergerMark": "sysapi1073",
            "paramStr": f'scode = {symbol}'
        }
        # response = requests.post(path, data=data, headers=headers)
        response = requests.post(path, headers=headers)
        dividend_and_split_list = response.json()

        for record in dividend_and_split_list:
            for key in record.keys():
                record[field_mapping[key]] = record.pop(key)

        collection = "cninfo_dividend"
        query = {
            "symbol": symbol
        }
        self.delete_and_save_dataframe_to_collection(collection, dividend, query)

    def save_dividend_from_csv(self, symbol, path="c:/gitrepo/backtest/data"):
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
        sort = [("record-date", ASCENDING)]
        dividend_history = self.db[collection].find(query).sort(sort)
        return dividend_history

    def get_finance_report_download_url(self, exchange, symbol, category, start_date='2000-01-01', end_date=None):
        """
        list designated finance report url
        reference:
            https://blog.csdn.net/herr_kun/article/details/89707078
            https://github.com/herrkun/Financial-data-collection-from-web-/blob/master/download_filesFromcsv_wyk.py
        :param exchange: 'sse', 'szse'
        :param symbol: stock code which consists of 6 digits
        :param category:
                annual finance report category_ndbg_szsh;
                half-annual finance report category_bndbg_szsh;
                1st quarter finance report category_yjdbg_szsh;
                3rd quarter finance report category_sjdbg_szsh;
                ipo report category_sf_szsh;
                业绩预告 category_yjygjxz_szsh;
                权益分派 category_qyfpxzcs_szsh;
                债券 category_zq_szsh;

        :return: list of the download url and file name
        """

        endpoint = f"{self.base_url}/new/hisAnnouncement/query"

        end_date = end_date if end_date else date.today().strftime("%Y-%m-%d")

        PAGE_SIZE = 30

        page = 1

        data = {
            'pageNum': page,
            'pageSize': PAGE_SIZE,
            'tabName': 'fulltext',
            'column': exchange,
            'stock': symbol,
            'searchkey': '',
            'plate': '',
            'category': category,
            'seDate': f"{start_date}~{end_date}"
        }

        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1) Chrome/58.0.3029.110',
            'X-Requested-With': 'XMLHttpRequest'
        }

        response = requests.post(endpoint, data=data, headers=headers)

        content = response.json()

        if content['totalAnnouncement'] == 0:
            return

        def __replace_abnormal_character(filename):
            illegal_char = {
                ' ': '',
                '*': '',
                '/': '-',
                '\\': '-',
                ':': '-',
                '?': '-',
                '"': '',
                '<': '',
                '>': '',
                '|': '',
                '－': '-',
                '—': '-',
                '（': '(',
                '）': ')',
                'Ａ': 'A',
                'Ｂ': 'B',
                'Ｈ': 'H',
                '，': ',',
                '。': '.',
                '：': '-',
                '！': '_',
                '？': '-',
                '“': '"',
                '”': '"',
                '‘': '',
                '’': ''
            }
            for abnormal_char, normal_char in illegal_char.items():
                filename = filename.replace(abnormal_char, normal_char)
            return filename

        report_url_prefix = 'http://static.cninfo.com.cn'

        reports = []

        for report in content['announcements']:
            report_name = __replace_abnormal_character(
                f"{report['secName']}{report['announcementTitle']}({report['adjunctSize']}K).{report['adjunctType']}"
            )
            report_url = f"{report_url_prefix}/{report['adjunctUrl']}"
            if '取消' not in report_name and '英文版' not in report_name and '摘要' not in report_name and '正文' not in report_name:
                reports.append((report_name, report_url))

        while content['hasMore']:
            page = page + 1
            data['pageNum'] = page + 1
            response = requests.post(endpoint, data=data, headers=headers)
            content = response.json()
            for report in content['announcements']:
                report_name = __replace_abnormal_character(
                    f"{report['secName']}{report['announcementTitle']}({report['adjunctSize']}K).{report['adjunctType']}"
                )
                report_url = f"{report_url_prefix}/{report['adjunctUrl']}"
                if '取消' not in report_name and '英文版' not in report_name:
                    reports.append((report_name, report_url))

        output_path = f"{SETTINGS.finance_report['home']}\\{symbol}\\{SETTINGS.finance_report['download_home']}"
        import os

        if not os.path.exists(output_path):
            os.makedirs(output_path)

        output_file = f"{output_path}\\{SETTINGS.finance_report['report_url_file']}"
        with open(output_file, 'w', newline='', encoding='gb18030') as output:
            writer = csv.writer(output)
            writer.writerows(reports)

    @staticmethod
    def download_report(symbol):
        download_path = f"{SETTINGS.finance_report['home']}\\{symbol}\\{SETTINGS.finance_report['download_home']}"
        report_urls_file = f"{download_path}\\{SETTINGS.finance_report['report_url_file']}"
        with open(report_urls_file, 'r') as input_file:
            reader = csv.reader(input_file)
            MAX_DOWNLOAD_COUNT = 5
            for report_name, report_url in reader:
                download_count = 1
                download_success = False
                while download_count <= MAX_DOWNLOAD_COUNT:
                    try:
                        download_count += 1
                        r = requests.get(report_url)
                        download_success = True
                        break
                    except:
                        # 下载失败则报错误
                        print(str(download_count) + ':: download' + report_name + ' failed!')
                        download_success = False
                        time.sleep(3)
                if download_success:
                    # 下载成功则保存
                    with open(download_path + '\\' + report_name, 'wb') as file:
                        file.write(r.content)
                        print(report_name + ' downloaded.')
                else:
                    # 彻底下载失败则记录日志
                    with open(download_path + '/' + 'error.log', 'a') as log_file:
                        log_file.write(
                            time.strftime('[%Y/%m/%d %H:%M:%S] ',
                                          time.localtime(time.time())) + 'Failed to download\"' +
                            report_name + '\"\n')
                        print('...' + report_name + ' finally failed ...')


def download_finance_report(exchange, symbols):
    import os
    cninfo_handler = CNInfoHandler()
    category = "category_ndbg_szsh;category_sjdbg_szsh;category_yjdbg_szsh;category_bndbg_szsh"

    for symbol in symbols:
        file_path = f"{SETTINGS.finance_report['home']}\\{symbol}"
        if not os.path.exists(file_path):
            os.makedirs(file_path)
        cninfo_handler.get_finance_report_download_url(exchange, symbol, category)
        cninfo_handler.download_report(symbol)