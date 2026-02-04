from __future__ import annotations

from pathlib import Path

import pandas as pd

from stock_data.jobs.macro import run_macro
from stock_data.jobs.etf import run_etf
from stock_data.jobs.market import run_market
from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_macro_update_uses_incremental_start_and_merges(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    # Existing local snapshot up to 202508.
    existing = pd.DataFrame(
        [
            {"month": "202506", "nt_val": 100.0},
            {"month": "202507", "nt_val": 100.1},
            {"month": "202508", "nt_val": 100.2},
        ]
    )
    _write_parquet(store_dir / "parquet" / "cpi" / "latest.parquet", existing)

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            if api_name == "cn_cpi":
                # Return only the new tail; run_macro should merge to keep history.
                return pd.DataFrame(
                    [
                        {"month": "202509", "nt_val": 100.3},
                        {"month": "202510", "nt_val": 100.4},
                    ]
                )
            return pd.DataFrame()

    import stock_data.jobs.macro as macro_mod

    monkeypatch.setattr(macro_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_macro(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["cpi"],
        start_date=None,
        end_date="20251031",
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    assert calls, "expected at least one Tushare query"

    api_name, params = calls[0]
    assert api_name == "cn_cpi"
    # With existing max month=202508 and default 1-month backfill, we should not
    # request the whole 202506..202510 range.
    assert params.get("start_m") >= "202507"
    assert params.get("end_m") == "202510"

    out = pd.read_parquet(store_dir / "parquet" / "cpi" / "latest.parquet")
    assert sorted(out["month"].astype(str).tolist()) == ["202506", "202507", "202508", "202509", "202510"]


def test_etf_fund_nav_update_uses_start_end_date(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    # fund_basic is required to enumerate ETF codes.
    fund_basic = pd.DataFrame([{"ts_code": "510300.SH", "name": "test"}])
    _write_parquet(store_dir / "parquet" / "fund_basic" / "latest.parquet", fund_basic)

    # Existing local per-code history up to 20250101.
    existing = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "nav_date": "20241231", "adj_nav": 1.0},
            {"ts_code": "510300.SH", "nav_date": "20250101", "adj_nav": 1.1},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_nav" / "ts_code=510300_SH.parquet", existing)

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            if api_name == "fund_nav":
                return pd.DataFrame(
                    [
                        {"ts_code": "510300.SH", "nav_date": "20250102", "adj_nav": 1.2},
                        {"ts_code": "510300.SH", "nav_date": "20250103", "adj_nav": 1.3},
                    ]
                )
            return pd.DataFrame()

    import stock_data.jobs.etf as etf_mod

    monkeypatch.setattr(etf_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_etf(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["fund_nav"],
        start_date=None,
        end_date="20250131",
        refresh_days=None,
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    assert calls, "expected at least one Tushare query"

    nav_calls = [(api, p) for (api, p) in calls if api == "fund_nav"]
    assert nav_calls, "expected fund_nav query"
    api_name, params = nav_calls[0]
    assert api_name == "fund_nav"
    assert params.get("ts_code") == "510300.SH"
    assert params.get("start_date") == "20250101"
    assert params.get("end_date") == "20250131"

    out = pd.read_parquet(store_dir / "parquet" / "fund_nav" / "ts_code=510300_SH.parquet")
    assert sorted(out["nav_date"].astype(str).tolist()) == ["20241231", "20250101", "20250102", "20250103"]


def test_etf_fund_share_update_uses_start_end_date(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    fund_basic = pd.DataFrame([{"ts_code": "510300.SH", "name": "test"}])
    _write_parquet(store_dir / "parquet" / "fund_basic" / "latest.parquet", fund_basic)

    existing = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "trade_date": "20241231", "fd_share": 1.0},
            {"ts_code": "510300.SH", "trade_date": "20250101", "fd_share": 1.1},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_share" / "ts_code=510300_SH.parquet", existing)

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            if api_name == "fund_share":
                return pd.DataFrame(
                    [
                        {"ts_code": "510300.SH", "trade_date": "20250102", "fd_share": 1.2},
                        {"ts_code": "510300.SH", "trade_date": "20250103", "fd_share": 1.3},
                    ]
                )
            return pd.DataFrame()

    import stock_data.jobs.etf as etf_mod

    monkeypatch.setattr(etf_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_etf(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["fund_share"],
        start_date=None,
        end_date="20250131",
        refresh_days=None,
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    assert calls, "expected at least one Tushare query"

    share_calls = [(api, p) for (api, p) in calls if api == "fund_share"]
    assert share_calls, "expected fund_share query"
    api_name, params = share_calls[0]
    assert api_name == "fund_share"
    assert params.get("ts_code") == "510300.SH"
    assert params.get("start_date") == "20250101"
    assert params.get("end_date") == "20250131"

    out = pd.read_parquet(store_dir / "parquet" / "fund_share" / "ts_code=510300_SH.parquet")
    assert sorted(out["trade_date"].astype(str).tolist()) == ["20241231", "20250101", "20250102", "20250103"]


def test_index_daily_update_uses_start_end_date(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    # index_basic is required to enumerate index codes.
    index_basic = pd.DataFrame(
        [
            {"ts_code": "000001.SH", "market": "SSE", "name": "test"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "index_basic" / "latest.parquet", index_basic)

    # Existing local per-code history up to 20250101.
    existing = pd.DataFrame(
        [
            {"ts_code": "000001.SH", "trade_date": "20250101", "close": 1.0},
        ]
    )
    _write_parquet(store_dir / "parquet" / "index_daily" / "ts_code=000001_SH.parquet", existing)

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            if api_name == "trade_cal":
                # Ensure effective_end is 20250131.
                return pd.DataFrame(
                    [
                        {"cal_date": "20250131", "is_open": "1"},
                    ]
                )
            if api_name == "index_daily":
                # Return a tail that reaches effective_end to avoid stale-skip.
                return pd.DataFrame(
                    [
                        {"ts_code": "000001.SH", "trade_date": "20250131", "close": 2.0},
                    ]
                )
            return pd.DataFrame()

    import stock_data.jobs.market as market_mod

    monkeypatch.setattr(market_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_market(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["index_daily"],
        start_date=None,
        end_date="20250131",
        index_daily_refresh_days=None,
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    assert calls, "expected at least one Tushare query"

    index_calls = [(api, p) for (api, p) in calls if api == "index_daily"]
    assert index_calls, "expected index_daily query"
    api_name, params = index_calls[0]
    assert api_name == "index_daily"
    assert params.get("ts_code") == "000001.SH"
    assert params.get("start_date") == "20250101"
    assert params.get("end_date") == "20250131"

    out = pd.read_parquet(store_dir / "parquet" / "index_daily" / "ts_code=000001_SH.parquet")
    assert sorted(out["trade_date"].astype(str).tolist()) == ["20250101", "20250131"]


def test_index_daily_schedules_only_lagging_codes(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    index_basic = pd.DataFrame(
        [
            {"ts_code": "000001.SH", "market": "SSE", "name": "ok"},
            {"ts_code": "000002.SH", "market": "SSE", "name": "lag"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "index_basic" / "latest.parquet", index_basic)

    # Code 000001.SH is already caught up; 000002.SH is behind.
    _write_parquet(
        store_dir / "parquet" / "index_daily" / "ts_code=000001_SH.parquet",
        pd.DataFrame([{ "ts_code": "000001.SH", "trade_date": "20250131", "close": 1.0 }]),
    )
    _write_parquet(
        store_dir / "parquet" / "index_daily" / "ts_code=000002_SH.parquet",
        pd.DataFrame([{ "ts_code": "000002.SH", "trade_date": "20250101", "close": 1.0 }]),
    )

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            if api_name == "trade_cal":
                return pd.DataFrame([{ "cal_date": "20250131", "is_open": "1" }])
            if api_name == "index_daily":
                # Only return data for the requested ts_code.
                code = params.get("ts_code")
                return pd.DataFrame([{ "ts_code": code, "trade_date": "20250131", "close": 2.0 }])
            return pd.DataFrame()

    import stock_data.jobs.market as market_mod

    monkeypatch.setattr(market_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_market(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["index_daily"],
        start_date=None,
        end_date="20250131",
        index_daily_refresh_days=None,
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    index_calls = [p for (api, p) in calls if api == "index_daily"]
    assert len(index_calls) == 1
    assert index_calls[0].get("ts_code") == "000002.SH"


def test_etf_fund_share_schedules_only_lagging_codes(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    fund_basic = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "name": "ok"},
            {"ts_code": "560000.SH", "name": "lag"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_basic" / "latest.parquet", fund_basic)

    # 510300.SH is caught up; 560000.SH is behind.
    _write_parquet(
        store_dir / "parquet" / "fund_share" / "ts_code=510300_SH.parquet",
        pd.DataFrame([{ "ts_code": "510300.SH", "trade_date": "20250131", "fd_share": 1.0 }]),
    )
    _write_parquet(
        store_dir / "parquet" / "fund_share" / "ts_code=560000_SH.parquet",
        pd.DataFrame([{ "ts_code": "560000.SH", "trade_date": "20250101", "fd_share": 1.0 }]),
    )

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            if api_name == "trade_cal":
                return pd.DataFrame([{ "cal_date": "20250131", "is_open": "1" }])
            if api_name == "fund_share":
                code = params.get("ts_code")
                return pd.DataFrame([{ "ts_code": code, "trade_date": "20250131", "fd_share": 2.0 }])
            return pd.DataFrame()

    import stock_data.jobs.etf as etf_mod

    monkeypatch.setattr(etf_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_etf(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["fund_share"],
        start_date=None,
        end_date="20250131",
        refresh_days=None,
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    fs_calls = [p for (api, p) in calls if api == "fund_share"]
    assert len(fs_calls) == 1
    assert fs_calls[0].get("ts_code") == "560000.SH"


def test_etf_fund_div_does_not_rerun_without_force_refresh(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    fund_basic = pd.DataFrame([
        {"ts_code": "510300.SH", "name": "a"},
        {"ts_code": "560000.SH", "name": "b"},
    ])
    _write_parquet(store_dir / "parquet" / "fund_basic" / "latest.parquet", fund_basic)

    # Existing local files for both codes.
    _write_parquet(
        store_dir / "parquet" / "fund_div" / "ts_code=510300_SH.parquet",
        pd.DataFrame([{ "ts_code": "510300.SH", "ann_date": "20240101" }]),
    )
    _write_parquet(
        store_dir / "parquet" / "fund_div" / "ts_code=560000_SH.parquet",
        pd.DataFrame([{ "ts_code": "560000.SH", "ann_date": "20240101" }]),
    )

    class FakeTushareClient:
        last_instance: "FakeTushareClient | None" = None

        def __init__(self, *args, **kwargs):
            self.calls: list[tuple[str, dict]] = []
            FakeTushareClient.last_instance = self

        def query(self, api_name: str, **params):
            self.calls.append((api_name, dict(params)))
            return pd.DataFrame()

    import stock_data.jobs.etf as etf_mod

    monkeypatch.setattr(etf_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_etf(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["fund_div"],
        start_date=None,
        end_date="20250131",
        refresh_days=None,
    )

    assert FakeTushareClient.last_instance is not None
    calls = FakeTushareClient.last_instance.calls
    assert not [1 for (api, _) in calls if api == "fund_div"], "should not query fund_div when already present"


def test_etf_empty_response_does_not_overwrite_existing(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    cfg = RunConfig(store_dir=str(store_dir), rpm=1, workers=1)

    fund_basic = pd.DataFrame([{"ts_code": "510300.SH", "name": "test"}])
    _write_parquet(store_dir / "parquet" / "fund_basic" / "latest.parquet", fund_basic)

    existing = pd.DataFrame(
        [
            {"ts_code": "510300.SH", "trade_date": "20250101", "fd_share": 1.0},
        ]
    )
    _write_parquet(store_dir / "parquet" / "fund_share" / "ts_code=510300_SH.parquet", existing)

    class FakeTushareClient:
        def __init__(self, *args, **kwargs):
            pass

        def query(self, api_name: str, **params):
            # trade_cal used for effective_end; return one open day.
            if api_name == "trade_cal":
                return pd.DataFrame([{ "cal_date": "20250131", "is_open": "1" }])
            if api_name == "fund_share":
                return pd.DataFrame()  # empty upstream response
            return pd.DataFrame()

    import stock_data.jobs.etf as etf_mod

    monkeypatch.setattr(etf_mod, "TushareClient", FakeTushareClient)

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    run_etf(
        cfg,
        token="dummy",
        catalog=cat,
        datasets=["fund_share"],
        start_date=None,
        end_date="20250131",
        refresh_days=None,
    )

    out = pd.read_parquet(store_dir / "parquet" / "fund_share" / "ts_code=510300_SH.parquet")
    assert sorted(out["trade_date"].astype(str).tolist()) == ["20250101"]
