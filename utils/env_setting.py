import yaml

from jinja2 import Template

CFG_FILE = r"c:\gitrepo\backtest\cfg.yml"


class Settings:

    def __init__(self, env=None):
        cfg_data = self._parse_config(env)
        self.mongodb = cfg_data.get("mongodb")

    def _parse_config(self, env=None):
        with open(CFG_FILE, 'r') as f:
            file_data = f.read()
            template = Template(file_data)
            data = template.render(env=env.lower())

        cfg_data = yaml.load(data)
        return cfg_data
