"""Local Tushare data lake (DuckDB + Parquet)."""

__all__ = ["__version__", "open_store", "StockStore", "ResolvedSymbol"]

__version__ = "0.1.0"

# Optional convenience imports (pandas-first data access).
from stock_data.store import ResolvedSymbol, StockStore, open_store  # noqa: E402,F401

