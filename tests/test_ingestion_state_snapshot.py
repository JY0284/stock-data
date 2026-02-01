from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from stock_data.storage.duckdb_catalog import DuckDBCatalog


def test_export_ingestion_state_snapshot(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    parquet_root = store_dir / "parquet"
    duckdb_path = store_dir / "duckdb" / "market.duckdb"

    cat = DuckDBCatalog(str(duckdb_path), str(parquet_root))
    cat.ensure_schema()

    cat.set_state(dataset="daily", partition_key="20260101", status="completed", row_count=123)
    cat.set_state(dataset="daily", partition_key="20260102", status="failed", row_count=0, error="boom")

    out = cat.export_ingestion_state_snapshot()
    assert out is not None
    assert Path(out).exists()

    df = pd.read_parquet(out)
    assert set(df.columns) >= {"dataset", "partition_key", "status"}

    # Ensure query can consume it as parquet.
    con = duckdb.connect(":memory:")
    try:
        path_sql = str(out).replace("'", "''")
        con.execute(
            f"CREATE VIEW v_ingestion_state AS SELECT * FROM read_parquet('{path_sql}', union_by_name=true)"
        )
        n = con.execute("SELECT COUNT(*) FROM v_ingestion_state WHERE dataset='daily'").fetchone()[0]
        assert int(n) == 2
    finally:
        con.close()
