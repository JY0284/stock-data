from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from stock_data.web_service import WebSettings, create_app


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_web_service_us_endpoints(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"

    us_basic = pd.DataFrame(
        [
            {"ts_code": "AAPL", "name": "苹果", "enname": "Apple Inc.", "classify": "EQ", "list_date": "19801212", "delist_date": ""},
            {"ts_code": "MSFT", "name": "微软", "enname": "Microsoft Corporation", "classify": "EQ", "list_date": "19860313", "delist_date": ""},
        ]
    )
    _write_parquet(store_dir / "parquet" / "us_basic" / "latest.parquet", us_basic)

    us_tradecal = pd.DataFrame(
        [
            {"cal_date": "20260102", "is_open": 1, "pretrade_date": "20251231"},
            {"cal_date": "20260103", "is_open": 0, "pretrade_date": "20260102"},
        ]
    )
    _write_parquet(store_dir / "parquet" / "us_tradecal" / "latest.parquet", us_tradecal)

    us_daily_20260102 = pd.DataFrame(
        [
            {"ts_code": "AAPL", "trade_date": "20260102", "open": 200.0, "high": 210.0, "low": 198.0, "close": 208.0, "vol": 100.0},
            {"ts_code": "MSFT", "trade_date": "20260102", "open": 300.0, "high": 305.0, "low": 295.0, "close": 302.0, "vol": 200.0},
        ]
    )
    _write_parquet(
        store_dir / "parquet" / "us_daily" / "year=2026" / "month=01" / "trade_date=20260102.parquet",
        us_daily_20260102,
    )

    app = create_app(settings=WebSettings(store_dir=str(store_dir), cache_enabled=False))
    with TestClient(app) as client:
        r = client.get("/datasets")
        assert r.status_code == 200
        names = {d["name"] for d in r.json()["datasets"]}
        assert "us_basic" in names
        assert "us_tradecal" in names
        assert "us_daily" in names

        r = client.get("/us")
        assert r.status_code == 200
        assert "us_daily" in r.json()["datasets"]

        r = client.get("/us/basic", params={"limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "us_basic"
        assert body["rows"] == 2

        r = client.get("/us/tradecal", params={"is_open": 1, "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "us_tradecal"
        assert body["rows"] == 1
        assert body["data"][0]["cal_date"] == "20260102"

        r = client.get("/us/daily", params={"ts_code": "AAPL", "start_date": "20260101", "end_date": "20260110", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "us_daily"
        assert body["ts_code"] == "AAPL"
        assert body["rows"] == 1
        assert body["data"][0]["trade_date"] == "20260102"

        # /query should also work
        r = client.get("/query", params={"dataset": "us_basic", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "us_basic"
        assert body["rows"] == 2
