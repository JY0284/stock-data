from stock_data.jobs.us import _normalize_trade_date_partition_key


def test_normalize_trade_date_partition_key() -> None:
    assert _normalize_trade_date_partition_key(None) is None
    assert _normalize_trade_date_partition_key("") is None
    assert _normalize_trade_date_partition_key("20260207") == "20260207"
    assert _normalize_trade_date_partition_key("trade_date=20260207") == "20260207"
    assert _normalize_trade_date_partition_key("trade_date=20060329") == "20060329"
