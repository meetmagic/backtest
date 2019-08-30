import os

import yaml

from jinja2 import Template

CFG_FILE = r"c:\gitrepo\backtest\cfg.yml"


class Settings:

    def __init__(self):
        cfg_data = self._parse_config()
        self.mongodb = cfg_data.get("mongodb")
        self.tushare = cfg_data.get("tushare")

    def _parse_config(self):
        env = os.getenv("backtestEnv", "")
        with open(CFG_FILE, "r") as f:
            file_data = f.read()
            template = Template(file_data)
            data = template.render(env=(env.lower() if env else ""))

        cfg_data = yaml.load(data, Loader=yaml.FullLoader)
        return cfg_data


SETTINGS = Settings()