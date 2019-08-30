class TestFromCNInfor:

    def test_save_dividend(self, cninfohandler):
        cninfohandler.save_dividend("601288")
        collection = "cninfo_dividend"
        query = {
            "symbol": "601288",
            "announcement_date": "20160630"
        }
        result = cninfohandler.db[collection].find_one(query)
        assert result["ex_date"] == "20160707"
        assert result["record_date"] == "20160706"
        assert result["dividend_payment_date"] == "20160707"
        assert result["dividend"] == 0.167
