from pytest import mark


@mark.foundationtest
class TestSettings:

    def test_mongodb_setup(self, mongodb):
        client = "127.0.0.1:27017"
        database = "backtestdev"
        username = "backtestdev"
        assert mongodb["client"] == client
        assert mongodb["database"] == database
        assert mongodb["username"] == username
