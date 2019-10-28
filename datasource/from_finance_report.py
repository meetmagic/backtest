from bs4 import BeautifulSoup

import re
import glob
import os

from utils.dbengine import MongoDatabase
from utils.env_setting import SETTINGS


class FinanceReportHandler(MongoDatabase):

    def __init__(self, symbol, report_type, year):
        self.symbol = symbol
        self.report_type = report_type
        self.year = year
        as_of_date_mapping = {
            'ANNUAL': '12-31',
            'SEMIANNUAL': '6-30',
            'Q1': '3-31',
            'Q3': '9-30'
        }
        self.as_of_date = f"{year}-{as_of_date_mapping[report_type]}"
        super(FinanceReportHandler, self).__init__()

    @classmethod
    def from_report_name(cls, symbol, report_name):

        if "半年度" in report_name:
            report_type = 'SEMIANNUAL'
        elif "年度" in report_name:
            report_type = 'ANNUAL'
        elif "第一季度" in report_name:
            report_type = 'Q1'
        elif "第三季度" in report_name:
            report_type = 'Q3'

        year = report_name[report_name.index('年')-4:report_name.index('年')]

        return cls(symbol, report_type, year)

    @property
    def report_name(self):
        file_name_mapping = {
            'ANNUAL': '年度',
            'SEMIANNUAL': '半年度',
            'Q1': '第一季度',
            'Q3': '第二季度'
        }
        pdf_home = f'{SETTINGS.finance_report["home"]}\\{self.symbol}\\{SETTINGS.finance_report["pdf_home"]}'
        file_list = glob.glob(f'{pdf_home}\\*{self.year}年{file_name_mapping[self.report_type]}*.PDF')
        if len(file_list) > 1:
            raise Exception("Finance report exceeds 1. Please confirm only one report in the folder.")
        return file_list[0].rsplit('\\', 1)[1].rsplit('.', 1)[0]

    def pdf2html(self, first_page=None, last_page=None):
        report_home = f'{SETTINGS.finance_report["home"]}\\{self.symbol}'
        pdf_file = f'{report_home}\\{SETTINGS.finance_report["pdf_home"]}\\{self.report_name}.PDF'
        html_home = f'{report_home}\\{SETTINGS.finance_report["html_home"]}'

        if os.path.exists(html_home):
            import shutil
            shutil.rmtree(html_home)
        os.mkdir(html_home)

        import subprocess
        cmd = ["c:/pdf2htmlEx/pdf2htmlEX.exe", '--embed-font', '0', '--embed-javascript', '0', '--embed-image', '0',
               '--dest', html_home, pdf_file]
        if first_page:
            cmd[1:1] = ['-f', str(first_page)]
        if last_page:
            cmd[1:1] = ['-l', str(last_page)]
        try:
            out_txt = subprocess.check_output(
                cmd,
                stderr=subprocess.STDOUT,
                shell=True
            ).decode('utf-8')
        except subprocess.CalledProcessError as e:
            out_txt = e.output.decode('utf-8')  # Output generated before error
            code = e.returncode  # Return code
            raise Exception(f"error code: {code}\nerror message: {out_txt}")

        return out_txt

    def _extract_tables(self, page):
        table_node = page.find(
            lambda tag: tag.name == 'div' and tag.has_attr('class') and tag['class'][0] == 'c'
        )

        has_table = True if table_node else False

        tables = []
        while has_table:

            _, column_identifier, row_identifier, *rest = table_node['class']
            table = {
                row_identifier: {column_identifier: table_node.text}
            }
            rows = [row_identifier]
            columns = [column_identifier]

            node = None

            for node in table_node.next_siblings:
                if node.has_attr('class') and node['class'][0] == 't':
                    break
                elif node.has_attr('class') and node['class'][0] == 'c':
                    _, column_identifier, row_identifier, *rest = node['class']
                    if row_identifier in table:
                        table[row_identifier][column_identifier] = node.text
                    else:
                        table[row_identifier] = {column_identifier: node.text}
                        rows.append(row_identifier)
                    if column_identifier not in columns:
                        columns.append(column_identifier)

            table_content = []
            for row in rows:
                row_content = []
                for column in columns:
                    row_content.append(table[row].get(column, None))
                table_content.append(tuple(row_content))

            formatted_table = {
                'table': table_content,
                'columns': columns
            }
            tables.append(formatted_table)

            if node:
                table_node = node.find_next_sibling(
                    lambda tag: tag.name == 'div' and tag.has_attr('class') and tag['class'][0] == 'c'
                )

                if not table_node:
                    has_table = False

            else:
                has_table = False

        return tables

    def export_tables(self):
        html_file = glob.glob(f'{SETTINGS.finance_report["home"]}\\{self.symbol}\\{SETTINGS.finance_report["html_home"]}\\*.html')[0]

        with open(html_file, 'r', encoding="utf-8") as f:
            html = re.sub(r'</*span.*?>', '', f.read(), flags=re.M)

        soup = BeautifulSoup(html, 'html.parser')

        page_container = soup.body.find('div', id='page-container')
        pages = page_container.find_all(
            lambda tag: tag.name == 'div' and tag.has_attr('data-page-no'),
            recursive=False
        )

        tables = []
        for page in pages:
            tables_in_page = self._extract_tables(page)

            if tables and tables_in_page and self._two_tables_are_same(tables_in_page[0]['columns'], tables[-1]['columns']):
                tables[-1]['table'].extend(tables_in_page[0]['table'])
                if len(tables_in_page[0]['columns']) > len(tables[-1]['columns']):
                    tables[-1]['columns'] = tables_in_page[0]['columns']

                del tables_in_page[0]

            tables.extend(tables_in_page)

        return tables

    def save_tables(self, tables):
        from pandas import DataFrame
        import pandas as pd
        excel_home = f'{SETTINGS.finance_report["home"]}\\{self.symbol}\\{SETTINGS.finance_report["excel_home"]}'
        if not os.path.exists(excel_home):
            os.mkdir(excel_home)

        excel_file = f'{excel_home}\\{self.symbol}_{self.year}_{self.report_type.lower()}_finance_report.xlsx'
        with pd.ExcelWriter(excel_file) as excel_writer:
            for index, table in enumerate(tables):
                df = DataFrame.from_records(table['table'])
                df.to_excel(excel_writer, str(index), header=False, index=False)

    def _two_tables_are_same(self, table_columns, other_table_columns):
        table_columns_set = set(table_columns)
        other_table_columns_set = set(other_table_columns)

        return True if table_columns_set >= other_table_columns_set or table_columns_set <= other_table_columns_set else False


def process_download_reports(symbol):
    download_path = f'{SETTINGS.finance_report["home"]}\\{symbol}\\{SETTINGS.finance_report["download_home"]}'

    report_files = glob.glob(f'{download_path}\\*.PDF')

    pdf_home = f'{SETTINGS.finance_report["home"]}\\{symbol}\\{SETTINGS.finance_report["pdf_home"]}'

    if not os.path.exists(pdf_home):
        os.makedirs(pdf_home)

    import shutil
    for report_file in report_files:
        shutil.move(report_file, pdf_home)
        report_handler = FinanceReportHandler.from_report_name(symbol, os.path.basename(report_file))
        # report_handler.pdf2html()
        tables = report_handler.export_tables()
        report_handler.save_tables(tables)

