

class TestBacktestDB:

    def test_backtest_db_initialization(self, backtestdb):
        collection_list = backtestdb.db.list_collection_names()
        assert isinstance(collection_list, list), "Did not connect to backtest database correctly"
