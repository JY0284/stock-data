from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_data import agent_tools


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _make_tiny_store(tmp_path: Path) -> Path:
    store_dir = tmp_path / "store"

    stock_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "name": "Ping An Bank",
                "industry": "Banking",
                "market": "主板",
                "list_date": "19910403",
                "list_status": "L",
                "exchange": "SZSE",
                "area": "广东",
            },
            {
                "ts_code": "000002.SZ",
                "symbol": "000002",
                "name": "Vanke",
                "industry": "RealEstate",
                "market": "主板",
                "list_date": "19910129",
                "list_status": "L",
                "exchange": "SZSE",
                "area": "广东",
            },
            {
                "ts_code": "600000.SH",
                "symbol": "600000",
                "name": "SPDB",
                "industry": "Banking",
                "market": "主板",
                "list_date": "19991110",
                "list_status": "D",
                "exchange": "SSE",
                "area": "上海",
            },
        ]
    )

    _write_parquet(store_dir / "parquet" / "stock_basic" / "latest.parquet", stock_basic)

    # Index basic snapshot
    index_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SH",
                "name": "SSE Composite",
                "market": "SSE",
                "publisher": "SSE",
                "category": "综合指数",
                "base_date": "19901219",
            }
        ]
    )
    _write_parquet(store_dir / "parquet" / "index_basic" / "latest.parquet", index_basic)

    # Index daily bars (ts_code-partitioned)
    index_daily = pd.DataFrame(
        [
            {
                "ts_code": "000001.SH",
                "trade_date": "20260103",
                "open": 3000.0,
                "high": 3050.0,
                "low": 2990.0,
                "close": 3040.0,
                "vol": 123456.0,
                "pct_chg": 0.8,
            },
            {
                "ts_code": "000001.SH",
                "trade_date": "20260102",
                "open": 2950.0,
                "high": 3010.0,
                "low": 2940.0,
                "close": 3000.0,
                "vol": 111111.0,
                "pct_chg": 1.2,
            },
        ]
    )
    _write_parquet(store_dir / "parquet" / "index_daily" / "ts_code=000001_SH.parquet", index_daily)

    # Fund basic snapshot
    fund_basic = pd.DataFrame(
        [
            {
                "ts_code": "510300.SH",
                "name": "CSI 300 ETF",
                "management": "TestManager",
                "fund_type": "ETF",
                "status": "L",
                "found_date": "20120528",
                "due_date": "",
                "list_date": "20120528",
            }
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_basic" / "latest.parquet", fund_basic)

    # ETF daily (trade_date-partitioned)
    etf_daily_20260102 = pd.DataFrame(
        [
            {
                "ts_code": "510300.SH",
                "trade_date": "20260102",
                "open": 4.0,
                "high": 4.1,
                "low": 3.9,
                "close": 4.05,
                "vol": 10000.0,
                "pct_chg": 0.5,
                "amount": 40500.0,
            },
            {
                "ts_code": "510500.SH",
                "trade_date": "20260102",
                "open": 1.0,
                "high": 1.1,
                "low": 0.9,
                "close": 1.05,
                "vol": 20000.0,
                "pct_chg": 1.0,
                "amount": 21000.0,
            },
        ]
    )
    _write_parquet(
        store_dir / "parquet" / "etf_daily" / "year=2026" / "month=01" / "trade_date=20260102.parquet",
        etf_daily_20260102,
    )

    # ETF/fund NAV (ts_code-partitioned)
    fund_nav = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "nav_date": "20260103", "unit_nav": 1.234, "accum_nav": 2.345, "adj_nav": 1.234},
            {"ts_code": "510300.SH", "nav_date": "20260102", "unit_nav": 1.200, "accum_nav": 2.300, "adj_nav": 1.200},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_nav" / "ts_code=510300_SH.parquet", fund_nav)

    # ETF shares outstanding (ts_code-partitioned)
    fund_share = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "trade_date": "20260103", "fd_share": 1000000.0, "fund_type": "ETF"},
            {"ts_code": "510300.SH", "trade_date": "20260102", "fd_share": 900000.0, "fund_type": "ETF"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_share" / "ts_code=510300_SH.parquet", fund_share)

    # ETF dividend distribution (ts_code-partitioned)
    fund_div = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "ann_date": "20260110", "div_cash": 0.01},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_div" / "ts_code=510300_SH.parquet", fund_div)

    # Finance datasets (end_date-partitioned)
    income = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "end_date": "20241231", "revenue": 100.0},
            {"ts_code": "000001.SZ", "end_date": "20240930", "revenue": 70.0},
        ]
    )
    _write_parquet(store_dir / "parquet" / "income" / "year=2024" / "income.parquet", income)

    balancesheet = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "end_date": "20241231", "total_assets": 999.0},
            {"ts_code": "000001.SZ", "end_date": "20240930", "total_assets": 900.0},
        ]
    )
    _write_parquet(store_dir / "parquet" / "balancesheet" / "year=2024" / "balancesheet.parquet", balancesheet)

    cashflow = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "end_date": "20241231", "n_cashflow_act": 12.0},
            {"ts_code": "000001.SZ", "end_date": "20240930", "n_cashflow_act": 8.0},
        ]
    )
    _write_parquet(store_dir / "parquet" / "cashflow" / "year=2024" / "cashflow.parquet", cashflow)

    fina_indicator = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "end_date": "20241231", "roe": 0.12},
            {"ts_code": "000001.SZ", "end_date": "20240930", "roe": 0.10},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fina_indicator" / "year=2024" / "fina_indicator.parquet", fina_indicator)

    # Finance audit opinions (ts_code-partitioned)
    fina_audit = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "end_date": "20241231", "audit_result": "unqualified"},
            {"ts_code": "000001.SZ", "end_date": "20231231", "audit_result": "unqualified"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fina_audit" / "ts_code=000001_SZ.parquet", fina_audit)

    fina_mainbz = pd.DataFrame(
        [
            {"ts_code": "000001.SZ", "end_date": "20241231", "bz_item": "segment_a", "bz_sales": 60.0},
            {"ts_code": "000001.SZ", "end_date": "20241231", "bz_item": "segment_b", "bz_sales": 40.0},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fina_mainbz" / "year=2024" / "fina_mainbz.parquet", fina_mainbz)

    # -----------------------------
    # US stock datasets
    # -----------------------------
    us_basic = pd.DataFrame(
        [
            {
                "ts_code": "AAPL",
                "name": "苹果",
                "enname": "Apple Inc.",
                "classify": "EQ",
                "list_date": "19801212",
                "delist_date": "",
            },
            {
                "ts_code": "MSFT",
                "name": "微软",
                "enname": "Microsoft Corporation",
                "classify": "EQ",
                "list_date": "19860313",
                "delist_date": "",
            },
        ]
    )
    _write_parquet(store_dir / "parquet" / "us_basic" / "latest.parquet", us_basic)

    us_tradecal = pd.DataFrame(
        [
            {"cal_date": "20260102", "is_open": 1, "pretrade_date": "20251231"},
            {"cal_date": "20260103", "is_open": 0, "pretrade_date": "20260102"},
            {"cal_date": "20260105", "is_open": 1, "pretrade_date": "20260102"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "us_tradecal" / "latest.parquet", us_tradecal)

    us_daily_20260102 = pd.DataFrame(
        [
            {
                "ts_code": "AAPL",
                "trade_date": "20260102",
                "open": 200.0,
                "high": 210.0,
                "low": 198.0,
                "close": 208.0,
                "vol": 100.0,
                "amount": 20800.0,
            },
            {
                "ts_code": "MSFT",
                "trade_date": "20260102",
                "open": 300.0,
                "high": 305.0,
                "low": 295.0,
                "close": 302.0,
                "vol": 200.0,
                "amount": 60400.0,
            },
        ]
    )
    _write_parquet(
        store_dir / "parquet" / "us_daily" / "year=2026" / "month=01" / "trade_date=20260102.parquet",
        us_daily_20260102,
    )

    us_daily_20260105 = pd.DataFrame(
        [
            {
                "ts_code": "AAPL",
                "trade_date": "20260105",
                "open": 208.0,
                "high": 212.0,
                "low": 207.0,
                "close": 211.0,
                "vol": 120.0,
                "amount": 25320.0,
            },
        ]
    )
    _write_parquet(
        store_dir / "parquet" / "us_daily" / "year=2026" / "month=01" / "trade_date=20260105.parquet",
        us_daily_20260105,
    )

    return store_dir


def test_resolve_symbol_and_get_stock_basic(tmp_path: Path) -> None:
    store_dir = _make_tiny_store(tmp_path)
    agent_tools.clear_store_cache(str(store_dir))

    r = agent_tools.resolve_symbol("000001", store_dir=str(store_dir))
    assert r["symbol"] == "000001"
    assert r["ts_code"] == "000001.SZ"

    out = agent_tools.get_stock_basic(limit=2, store_dir=str(store_dir))
    assert out["total_count"] == 3
    assert len(out["rows"]) == 2
    assert out["has_more"] is True

    out2 = agent_tools.get_stock_basic(name_contains="bank", limit=10, store_dir=str(store_dir))
    # Ping An Bank should match; case-insensitive
    assert out2["total_count"] == 1
    assert out2["rows"][0]["ts_code"] == "000001.SZ"


def test_get_universe_filters(tmp_path: Path) -> None:
    store_dir = _make_tiny_store(tmp_path)
    agent_tools.clear_store_cache(str(store_dir))

    out = agent_tools.get_universe(list_status="L", exchange="SZSE", limit=10, store_dir=str(store_dir))
    assert out["total_count"] == 2
    ts_codes = {r["ts_code"] for r in out["rows"]}
    assert ts_codes == {"000001.SZ", "000002.SZ"}


def test_index_tools(tmp_path: Path) -> None:
    store_dir = _make_tiny_store(tmp_path)
    agent_tools.clear_store_cache(str(store_dir))

    out = agent_tools.get_index_basic(limit=10, store_dir=str(store_dir))
    assert out["total_count"] == 1
    assert out["rows"][0]["ts_code"] == "000001.SH"

    bars = agent_tools.get_index_daily_prices("000001.SH", limit=10, store_dir=str(store_dir))
    assert bars["total_count"] == 2
    # Most recent first
    assert bars["rows"][0]["trade_date"] == "20260103"


def test_etf_tools(tmp_path: Path) -> None:
    store_dir = _make_tiny_store(tmp_path)
    agent_tools.clear_store_cache(str(store_dir))

    fb = agent_tools.get_fund_basic(limit=10, store_dir=str(store_dir))
    assert fb["total_count"] == 1
    assert fb["rows"][0]["ts_code"] == "510300.SH"

    daily = agent_tools.get_etf_daily_prices("510300.SH", limit=10, store_dir=str(store_dir))
    assert daily["total_count"] == 1
    assert daily["rows"][0]["trade_date"] == "20260102"

    nav = agent_tools.get_fund_nav("510300.SH", limit=10, store_dir=str(store_dir))
    assert nav["total_count"] == 2
    assert nav["rows"][0]["nav_date"] == "20260103"

    share = agent_tools.get_fund_share("510300.SH", limit=10, store_dir=str(store_dir))
    assert share["total_count"] == 2
    assert share["rows"][0]["trade_date"] == "20260103"

    div = agent_tools.get_fund_div("510300.SH", limit=10, store_dir=str(store_dir))
    assert div["total_count"] == 1


def test_finance_tools(tmp_path: Path) -> None:
    store_dir = _make_tiny_store(tmp_path)
    agent_tools.clear_store_cache(str(store_dir))

    inc = agent_tools.get_income("000001.SZ", limit=10, store_dir=str(store_dir))
    assert inc["total_count"] == 2
    assert inc["rows"][0]["end_date"] == "20241231"

    bs = agent_tools.get_balancesheet("000001.SZ", limit=10, store_dir=str(store_dir))
    assert bs["total_count"] == 2
    assert bs["rows"][0]["end_date"] == "20241231"

    cf = agent_tools.get_cashflow("000001.SZ", limit=10, store_dir=str(store_dir))
    assert cf["total_count"] == 2
    assert cf["rows"][0]["end_date"] == "20241231"

    ind = agent_tools.get_fina_indicator("000001.SZ", limit=10, store_dir=str(store_dir))
    assert ind["total_count"] == 2
    assert ind["rows"][0]["end_date"] == "20241231"

    audit = agent_tools.get_fina_audit("000001.SZ", limit=10, store_dir=str(store_dir))
    assert audit["total_count"] == 2
    assert audit["rows"][0]["end_date"] == "20241231"

    mb = agent_tools.get_fina_mainbz("000001.SZ", limit=10, store_dir=str(store_dir))
    assert mb["total_count"] == 2
    assert mb["rows"][0]["end_date"] == "20241231"


def test_us_tools(tmp_path: Path, monkeypatch: "pytest.MonkeyPatch") -> None:
    store_dir = _make_tiny_store(tmp_path)

    # Ensure this integration test is independent of repo-level stock_data.yaml.
    # We explicitly enable everything (default behavior) by pointing at an empty config.
    cfg_path = tmp_path / "stock_data_test.yaml"
    cfg_path.write_text("{}\n", encoding="utf-8")
    monkeypatch.setenv("STOCK_DATA_CONFIG", str(cfg_path))

    agent_tools.clear_store_cache(str(store_dir))

    basics = agent_tools.get_us_basic(limit=10, store_dir=str(store_dir))
    assert basics["total_count"] == 2
    codes = {r["ts_code"] for r in basics["rows"]}
    assert codes == {"AAPL", "MSFT"}

    aapl = agent_tools.get_us_basic_detail("AAPL", store_dir=str(store_dir))
    assert aapl["found"] is True
    assert aapl["data"]["ts_code"] == "AAPL"

    cal = agent_tools.get_us_tradecal(start_date="20260101", end_date="20260110", is_open=1, limit=10, store_dir=str(store_dir))
    assert cal["total_count"] == 2
    assert cal["rows"][0]["cal_date"] == "20260105"  # sorted desc

    bars = agent_tools.get_us_daily_prices("AAPL", start_date="20260101", end_date="20260110", limit=10, store_dir=str(store_dir))
    assert bars["total_count"] == 2
    assert bars["rows"][0]["trade_date"] == "20260105"  # most recent first
