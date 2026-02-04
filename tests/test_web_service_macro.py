from __future__ import annotations

from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from stock_data.web_service import WebSettings, create_app


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_web_service_macro_endpoints(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"

    lpr = pd.DataFrame([
        {"date": "20260101", "1y": 3.1, "5y": 3.5},
        {"date": "20260102", "1y": 3.2, "5y": 3.6},
    ])
    _write_parquet(store_dir / "parquet" / "lpr" / "latest.parquet", lpr)

    app = create_app(settings=WebSettings(store_dir=str(store_dir), cache_enabled=False))
    with TestClient(app) as client:
        r = client.get("/datasets")
        assert r.status_code == 200
        names = {d["name"] for d in r.json()["datasets"]}
        assert "lpr" in names

        r = client.get("/query", params={"dataset": "lpr", "limit": 10})
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "lpr"
        assert body["rows"] == 2

        r = client.get("/macro")
        assert r.status_code == 200
        assert "lpr" in r.json()["datasets"]

        r = client.get(
            "/macro/lpr",
            params={"where": '{"date":"20260102"}', "columns": "date,1y", "limit": 10},
        )
        assert r.status_code == 200
        body = r.json()
        assert body["dataset"] == "lpr"
        assert body["rows"] == 1
        assert body["data"][0]["date"] == "20260102"
