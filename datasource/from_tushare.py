import tushare


class TushareHanlder:

    def __init__(self, token):
        self.ts_pro = tushare.pro_api(token)
