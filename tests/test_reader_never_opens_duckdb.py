from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import duckdb
import pandas as pd

from stock_data.runner import run_command
from stock_data.store import open_store


def _write_parquet(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


def test_readers_never_open_on_disk_duckdb(tmp_path: Path, monkeypatch) -> None:
    store_dir = tmp_path / "store"
    parquet_dir = store_dir / "parquet"
    duckdb_path = store_dir / "duckdb" / "market.duckdb"

    # Minimal parquet data for views + store.
    stock_basic = pd.DataFrame(
        [
            {
                "ts_code": "000001.SZ",
                "symbol": "000001",
                "name": "Ping An Bank",
                "list_date": "19910403",
                "list_status": "L",
                "exchange": "SZSE",
            }
        ]
    )
    _write_parquet(parquet_dir / "stock_basic" / "latest.parquet", stock_basic)

    # Guard duckdb.connect so any attempt to open the on-disk DB fails the test.
    real_connect = duckdb.connect

    def guarded_connect(database: str, *args, **kwargs):
        assert str(database) != str(duckdb_path)
        return real_connect(database, *args, **kwargs)

    monkeypatch.setattr(duckdb, "connect", guarded_connect)

    # Reader path: should work and must not touch duckdb_path.
    store = open_store(str(store_dir))
    try:
        sym = store.resolve("000001")
        assert sym.ts_code == "000001.SZ"
    finally:
        store.close()

    # Query path: should work and must not touch duckdb_path.
    args = SimpleNamespace(cmd="query", store=str(store_dir), rpm=1, workers=1, sql="SELECT count(*) AS n FROM v_stock_basic")
    rc = run_command(args, token="")
    assert rc == 0
