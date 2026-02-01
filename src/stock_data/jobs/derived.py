from __future__ import annotations

from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.storage.parquet_views import register_parquet_views


def ensure_derived_views(catalog: DuckDBCatalog) -> None:
    """Create convenience DuckDB views over parquet datasets.

    This writes views into the catalog's DuckDB connection (typically on-disk).
    Reader codepaths should create ephemeral views in an in-memory DuckDB instead.
    """
    with catalog.connect() as con:
        register_parquet_views(con, parquet_root=catalog.parquet_root, also_create_dataset_views=False)

