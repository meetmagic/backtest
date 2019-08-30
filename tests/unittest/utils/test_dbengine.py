from pytest import mark


@mark.foundationtest
class TestDBEngine:

    def test_mongodb_client(self, mongodbclient):
        db = mongodbclient.client.get_database()
        collection = db.test
        collection.insert_one({"key": "value"})
        item = collection.find_one()
        item.pop("_id", None)
        assert item == {"key": "value"}
        collection.drop()

