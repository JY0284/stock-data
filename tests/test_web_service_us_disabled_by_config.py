from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from stock_data.web_service import WebSettings, create_app


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def _set_test_config(monkeypatch, tmp_path: Path, yaml_text: str) -> None:
    cfg_path = tmp_path / "stock_data_test_config.yaml"
    cfg_path.write_text(yaml_text, encoding="utf-8")
    monkeypatch.setenv("STOCK_DATA_CONFIG", str(cfg_path))
    from stock_data.config import clear_config_cache

    clear_config_cache()


def test_web_service_us_not_exposed_when_disabled(tmp_path: Path, monkeypatch) -> None:
    # Disable all US datasets via category shorthand
    _set_test_config(
        monkeypatch,
        tmp_path,
        """
categories:
    us: false
    us_index: false
""".lstrip(),
    )

    store_dir = tmp_path / "store"

    # Even if parquet exists, the API should not expose it.
    us_basic = pd.DataFrame([
        {"ts_code": "AAPL", "name": "苹果", "enname": "Apple Inc.", "classify": "EQ", "list_date": "19801212", "delist_date": ""},
    ])
    _write_parquet(store_dir / "parquet" / "us_basic" / "latest.parquet", us_basic)

    app = create_app(settings=WebSettings(store_dir=str(store_dir), cache_enabled=False))
    with TestClient(app) as client:
        r = client.get("/datasets")
        assert r.status_code == 200
        names = {d["name"] for d in r.json()["datasets"]}
        assert "us_basic" not in names
        assert "us_tradecal" not in names
        assert "us_daily" not in names

        r = client.get("/us")
        assert r.status_code == 200
        assert r.json()["datasets"] == []
        assert r.json()["count"] == 0

        # Specific endpoints blocked
        r = client.get("/us/basic", params={"limit": 10})
        assert r.status_code == 400

        r = client.get("/us/tradecal", params={"is_open": 1, "limit": 10})
        assert r.status_code == 400

        r = client.get("/us/daily", params={"ts_code": "AAPL", "start_date": "20260101", "end_date": "20260110", "limit": 10})
        assert r.status_code == 400

        # Generic /query blocked
        r = client.get("/query", params={"dataset": "us_basic", "limit": 10})
        assert r.status_code == 400
