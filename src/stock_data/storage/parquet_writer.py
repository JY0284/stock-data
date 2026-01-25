from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from typing import Optional


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _atomic_replace(src: str, dst: str) -> None:
    os.replace(src, dst)


def _trade_date_to_parts(trade_date: str) -> tuple[str, str]:
    yyyy = trade_date[:4]
    mm = trade_date[4:6]
    return yyyy, mm


@dataclass(frozen=True)
class ParquetWriter:
    base_dir: str

    def dataset_dir(self, dataset: str) -> str:
        return os.path.join(self.base_dir, dataset)

    def write_snapshot(self, dataset: str, df, *, name: str = "latest") -> str:
        """
        Writes a single parquet snapshot file: <base>/<dataset>/<name>.parquet
        """
        ddir = self.dataset_dir(dataset)
        _ensure_dir(ddir)
        final_path = os.path.join(ddir, f"{name}.parquet")
        return self._write_atomic(df, final_path)

    def write_trade_date_partition(self, dataset: str, trade_date: str, df) -> str:
        """
        Writes a date-partitioned file:
          <base>/<dataset>/year=YYYY/month=MM/trade_date=YYYYMMDD.parquet

        We overwrite partitions idempotently (atomic replace).
        """
        # Some API calls may return None (or an empty frame without schema).
        # DuckDB cannot read Parquet files with zero columns, so ensure at least
        # a stable partition column exists.
        try:
            import pandas as pd

            if df is None:
                df = pd.DataFrame({"trade_date": pd.Series(dtype="string")})
            elif isinstance(df, pd.DataFrame) and df.empty and len(df.columns) == 0:
                df = pd.DataFrame({"trade_date": pd.Series(dtype="string")})
        except Exception:
            # If pandas isn't available for some reason, let _write_atomic raise
            # a clearer error later.
            pass

        yyyy, mm = _trade_date_to_parts(trade_date)
        ddir = os.path.join(self.dataset_dir(dataset), f"year={yyyy}", f"month={mm}")
        _ensure_dir(ddir)
        final_path = os.path.join(ddir, f"trade_date={trade_date}.parquet")
        return self._write_atomic(df, final_path)

    def write_end_date_partition(self, dataset: str, end_date: str, df) -> str:
        """
        Writes a report-period-partitioned file for financial data:
          <base>/<dataset>/year=YYYY/quarter=Q/end_date=YYYYMMDD.parquet

        We overwrite partitions idempotently (atomic replace).
        """
        # Some API calls may return None (or an empty frame without schema).
        # DuckDB cannot read Parquet files with zero columns, so ensure at least
        # a stable partition column exists.
        try:
            import pandas as pd

            if df is None:
                df = pd.DataFrame({"end_date": pd.Series(dtype="string")})
            elif isinstance(df, pd.DataFrame) and df.empty and len(df.columns) == 0:
                df = pd.DataFrame({"end_date": pd.Series(dtype="string")})
        except Exception:
            # If pandas isn't available for some reason, let _write_atomic raise
            # a clearer error later.
            pass

        yyyy = end_date[:4]
        mm = end_date[4:6]
        q = (int(mm) - 1) // 3 + 1
        ddir = os.path.join(self.dataset_dir(dataset), f"year={yyyy}", f"quarter={q}")
        _ensure_dir(ddir)
        final_path = os.path.join(ddir, f"end_date={end_date}.parquet")
        return self._write_atomic(df, final_path)

    def _write_atomic(self, df, final_path: str) -> str:
        # Import lazily to keep CLI help lightweight.
        import pandas as pd

        if df is None:
            raise ValueError("df is None")
        if not isinstance(df, pd.DataFrame):
            df = pd.DataFrame(df)

        # Avoid producing Parquet files with zero columns (DuckDB can't read them).
        if len(df.columns) == 0:
            df = pd.DataFrame({"_empty": pd.Series(dtype="int8")})

        tmp_dir = os.path.dirname(final_path)
        _ensure_dir(tmp_dir)
        fd, tmp_path = tempfile.mkstemp(prefix=".tmp-", suffix=".parquet", dir=tmp_dir)
        os.close(fd)
        try:
            df.to_parquet(tmp_path, engine="pyarrow", index=False)
            _atomic_replace(tmp_path, final_path)
        finally:
            if os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except OSError:
                    pass
        return final_path

