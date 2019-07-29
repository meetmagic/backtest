import pytest
from utils import dbengine


class TestBacktestDB:

    def test_backtest_db_initialization(self):
        backtest_db = dbengine.BacktestDB()
        collection_list = backtest_db.db.list_collection_names
        assert 'backtest' in collection_list, 'mongodb client was not initialized correctly'
