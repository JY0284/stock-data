from __future__ import annotations

import datetime as _dt
import os
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Iterable, Literal

import pandas as pd

from stock_data.utils_dates import parse_yyyymmdd

How = Literal["qfq", "hfq", "both"]


@dataclass(frozen=True)
class ResolvedSymbol:
    symbol: str
    ts_code: str
    list_date: str | None


def open_store(
    store_dir: str = "store",
    *,
    cache: bool = True,
    cache_budget_bytes: int = 1_000_000_000,  # ~1GB by default
    cache_ttl_seconds: int = 10 * 60,  # 10 minutes
    exchange_default: str = "SSE",
) -> "StockStore":
    return StockStore(
        store_dir=store_dir,
        cache=cache,
        cache_budget_bytes=cache_budget_bytes,
        cache_ttl_seconds=cache_ttl_seconds,
        exchange_default=exchange_default,
    )


class _LruTtlCache:
    """A small in-memory LRU cache with TTL and approximate byte budgeting."""

    def __init__(self, *, budget_bytes: int, ttl_seconds: int) -> None:
        self._budget_bytes = int(budget_bytes)
        self._ttl_seconds = int(ttl_seconds)
        self._lock = threading.RLock()
        self._data: "OrderedDict[str, tuple[float, int, Any]]" = OrderedDict()
        self._size_bytes = 0

    def clear(self) -> None:
        with self._lock:
            self._data.clear()
            self._size_bytes = 0

    def get(self, key: str) -> Any | None:
        now = time.time()
        with self._lock:
            item = self._data.get(key)
            if item is None:
                return None
            ts, size_b, val = item
            if self._ttl_seconds > 0 and (now - ts) > self._ttl_seconds:
                # Expired
                self._data.pop(key, None)
                self._size_bytes -= size_b
                return None
            # Refresh LRU
            self._data.move_to_end(key, last=True)
            return val

    def set(self, key: str, value: Any, *, size_bytes: int) -> None:
        size_b = max(0, int(size_bytes))
        now = time.time()
        with self._lock:
            old = self._data.pop(key, None)
            if old is not None:
                _ts, old_size_b, _old_val = old
                self._size_bytes -= old_size_b

            # If a single value exceeds budget, don't cache it.
            if self._budget_bytes > 0 and size_b > self._budget_bytes:
                return

            self._data[key] = (now, size_b, value)
            self._size_bytes += size_b
            self._data.move_to_end(key, last=True)

            # Evict LRU until within budget.
            while self._budget_bytes > 0 and self._size_bytes > self._budget_bytes and self._data:
                _k, (_ts, evict_b, _v) = self._data.popitem(last=False)
                self._size_bytes -= evict_b


class StockStore:
    """Dataset-centric access layer over the local store (DuckDB + Parquet).

    Design goals:
    - pandas-first API
    - resilient (works without v_* views)
    - performant for repeated requests (partition pruning + caching)
    """

    # Trade-date partitioned datasets in this repo.
    _TRADE_DATE_DATASETS: set[str] = {
        "daily",
        "adj_factor",
        "daily_basic",
        "stk_limit",
        "suspend_d",
        "weekly",
        "monthly",
    }

    # Snapshot datasets in this repo.
    _SNAPSHOT_DATASETS: set[str] = {"stock_basic", "stock_company", "trade_cal"}

    # Windowed-by-year datasets in this repo.
    _YEAR_WINDOW_DATASETS: set[str] = {"new_share", "namechange"}

    def __init__(
        self,
        *,
        store_dir: str,
        cache: bool,
        cache_budget_bytes: int,
        cache_ttl_seconds: int,
        exchange_default: str,
    ) -> None:
        self.store_dir = store_dir
        self.parquet_dir = os.path.join(store_dir, "parquet")
        self.duckdb_path = os.path.join(store_dir, "duckdb", "market.duckdb")

        self.exchange_default = exchange_default

        self._lock = threading.RLock()
        self._con = None

        # Metadata caches (always on; small).
        self._stock_basic_min: pd.DataFrame | None = None
        self._trade_cal: dict[str, pd.DataFrame] = {}
        self._trading_days: dict[tuple[str, str, str], list[str]] = {}

        # Result cache (configurable; large by default).
        self._cache_enabled = bool(cache)
        self._cache = _LruTtlCache(budget_bytes=cache_budget_bytes, ttl_seconds=cache_ttl_seconds)

        # Cache-busting hint: use DuckDB mtime if present (cheap).
        self._data_version_hint = self._compute_data_version_hint()

    # -----------------------------
    # Lifecycle / caching controls
    # -----------------------------
    def close(self) -> None:
        """Close the underlying DuckDB connection (optional)."""
        with self._lock:
            if self._con is not None:
                try:
                    self._con.close()
                finally:
                    self._con = None

    def clear_cache(self) -> None:
        """Clear only the result cache (not metadata caches)."""
        self._cache.clear()

    def refresh_version(self) -> None:
        """Refresh the data-version hint (useful after running `stock-data update`)."""
        self._data_version_hint = self._compute_data_version_hint()
        self.clear_cache()

    # -----------------------------
    # Public API: identity / basic
    # -----------------------------
    def resolve(self, symbol_or_ts_code: str) -> ResolvedSymbol:
        s = (symbol_or_ts_code or "").strip()
        if not s:
            raise ValueError("symbol_or_ts_code is required")

        # Heuristic: ts_code contains '.' and endswith .SZ/.SH/etc.
        if "." in s:
            ts_code = s
            df = self.stock_basic(ts_code=ts_code, columns=["ts_code", "symbol", "list_date"])
            if df.empty:
                raise RuntimeError(f"ts_code not found in stock_basic: {ts_code}")
            symbol = str(df.loc[0, "symbol"])
            list_date = df.loc[0, "list_date"]
            list_date = str(list_date) if pd.notna(list_date) and str(list_date).strip() else None
            return ResolvedSymbol(symbol=symbol, ts_code=ts_code, list_date=list_date)

        symbol = s
        df = self._stock_basic_minimal()
        hit = df.loc[df["symbol"] == symbol, ["ts_code", "list_date"]]
        if hit.empty:
            raise RuntimeError(f"symbol not found in stock_basic: {symbol}")
        ts_code = str(hit.iloc[0]["ts_code"])
        list_date = hit.iloc[0]["list_date"]
        list_date = str(list_date) if pd.notna(list_date) and str(list_date).strip() else None
        return ResolvedSymbol(symbol=symbol, ts_code=ts_code, list_date=list_date)

    def stock_basic(
        self,
        *,
        ts_code: str | None = None,
        symbol: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        df = self.read("stock_basic", columns=columns, limit=limit, cache=cache)
        if ts_code:
            df = df.loc[df["ts_code"] == ts_code]
        if symbol:
            df = df.loc[df["symbol"] == symbol]
        return df.reset_index(drop=True)

    def stock_company(
        self,
        *,
        ts_code: str,
        columns: list[str] | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        df = self.read("stock_company", columns=columns, cache=cache)
        return df.loc[df["ts_code"] == ts_code].reset_index(drop=True)

    def universe(
        self,
        *,
        list_status: str | None = "L",
        exchange: str | None = None,
        market: str | None = None,
        is_hs: str | None = None,
        industry: str | None = None,
        area: str | None = None,
        columns: list[str] | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        df = self.read("stock_basic", columns=columns, cache=cache)
        if list_status is not None:
            df = df.loc[df["list_status"] == list_status]
        if exchange is not None:
            df = df.loc[df["exchange"] == exchange]
        if market is not None:
            df = df.loc[df["market"] == market]
        if is_hs is not None:
            df = df.loc[df["is_hs"] == is_hs]
        if industry is not None:
            df = df.loc[df["industry"] == industry]
        if area is not None:
            df = df.loc[df["area"] == area]
        return df.reset_index(drop=True)

    # -----------------------------
    # Public API: calendar
    # -----------------------------
    def trade_cal(self, *, exchange: str | None = None, cache: bool = True) -> pd.DataFrame:
        ex = (exchange or self.exchange_default).strip()
        if cache and ex in self._trade_cal:
            return self._trade_cal[ex]
        df = self._read_trade_cal(exchange=ex, cache=cache)
        if cache:
            self._trade_cal[ex] = df
        return df

    def trading_days(self, start_date: str, end_date: str, *, exchange: str | None = None) -> list[str]:
        ex = (exchange or self.exchange_default).strip()
        key = (ex, start_date, end_date)
        cached = self._trading_days.get(key)
        if cached is not None:
            return cached

        cal = self.trade_cal(exchange=ex, cache=True)
        s = parse_yyyymmdd(start_date)
        e = parse_yyyymmdd(end_date)
        df = cal.loc[(cal["is_open"] == 1)]
        # cal_date is stored as YYYYMMDD string (or int); normalize to string for compare.
        dates = [str(x) for x in df["cal_date"].tolist()]
        out = sorted([d for d in dates if s <= parse_yyyymmdd(d) <= e])
        self._trading_days[key] = out
        return out

    def is_trading_day(self, date: str, *, exchange: str | None = None) -> bool:
        ex = (exchange or self.exchange_default).strip()
        cal = self.trade_cal(exchange=ex, cache=True)
        d = str(date)
        hit = cal.loc[cal["cal_date"].astype(str) == d, "is_open"]
        return bool(len(hit) > 0 and int(hit.iloc[0]) == 1)

    def prev_trade_date(self, date: str, *, exchange: str | None = None) -> str | None:
        ex = (exchange or self.exchange_default).strip()
        cal = self.trade_cal(exchange=ex, cache=True)
        d = str(date)
        hit = cal.loc[cal["cal_date"].astype(str) == d, "pretrade_date"]
        if hit.empty:
            return None
        v = hit.iloc[0]
        return str(v) if pd.notna(v) and str(v).strip() else None

    def next_trade_date(self, date: str, *, exchange: str | None = None) -> str | None:
        ex = (exchange or self.exchange_default).strip()
        # next_trade_date isn't stored; compute from open days.
        cal = self.trade_cal(exchange=ex, cache=True)
        d = str(date)
        df = cal.loc[cal["is_open"] == 1, "cal_date"].astype(str).sort_values()
        # Find first open day strictly greater than d.
        after = df.loc[df > d]
        return str(after.iloc[0]) if len(after) > 0 else None

    # -----------------------------
    # Public API: year-windowed datasets
    # -----------------------------
    def new_share(
        self,
        *,
        year: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        ts_code: str | None = None,
        symbol_or_sub_code: str | None = None,
        cache: bool = True,
        limit: int | None = None,
    ) -> pd.DataFrame:
        df = self._read_year_window_dataset("new_share", year=year, cache=cache)
        if start_date is not None:
            df = df.loc[df["ipo_date"].astype(str) >= str(start_date)]
        if end_date is not None:
            df = df.loc[df["ipo_date"].astype(str) <= str(end_date)]
        if ts_code is not None:
            df = df.loc[df["ts_code"] == ts_code]
        if symbol_or_sub_code is not None:
            v = str(symbol_or_sub_code)
            df = df.loc[(df["sub_code"].astype(str) == v) | (df["ts_code"].astype(str) == v)]
        if limit is not None:
            df = df.head(int(limit))
        return df.reset_index(drop=True)

    def namechange(
        self,
        *,
        ts_code: str,
        start_date: str | None = None,
        end_date: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        df = self._read_year_window_dataset("namechange", year=None, cache=cache)
        df = df.loc[df["ts_code"] == ts_code]
        if start_date is not None:
            df = df.loc[df["ann_date"].astype(str) >= str(start_date)]
        if end_date is not None:
            df = df.loc[df["ann_date"].astype(str) <= str(end_date)]
        if not df.empty and "ann_date" in df.columns:
            df = df.sort_values("ann_date")
        return df.reset_index(drop=True)

    # -----------------------------
    # Public API: market datasets
    # -----------------------------
    def daily(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "daily",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def adj_factor(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "adj_factor",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def daily_basic(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "daily_basic",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def weekly(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "weekly",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def monthly(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "monthly",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def stk_limit(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "stk_limit",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def suspend_d(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        return self._read_trade_date_dataset(
            "suspend_d",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by="trade_date",
            exchange=exchange,
            cache=cache,
        )

    def daily_adj(
        self,
        ts_code: str,
        *,
        start_date: str | None = None,
        end_date: str | None = None,
        how: How = "both",
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        how = how.lower().strip()  # type: ignore[assignment]
        if how not in {"qfq", "hfq", "both"}:
            raise ValueError("how must be one of: qfq, hfq, both")

        cache_key = self._cache_key(
            "daily_adj",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            how=how,
            exchange=exchange,
        )
        if cache and self._cache_enabled:
            hit = self._cache.get(cache_key)
            if hit is not None:
                return hit.copy()

        con = self._connect()
        daily_expr, daily_params = self._dataset_relation_expr(
            "daily",
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
        )
        adj_expr, adj_params = self._dataset_relation_expr(
            "adj_factor",
            start_date=start_date,
            end_date=end_date,
            exchange=exchange,
        )
        params = [*daily_params, *adj_params, ts_code]

        # Keep the computation consistent with src/stock_data/jobs/derived.py.
        # We compute both qfq/hfq columns, then optionally select a subset.
        sql = f"""
        WITH d AS (
          SELECT * FROM {daily_expr}
        ),
        a AS (
          SELECT * FROM {adj_expr}
        ),
        j AS (
          SELECT d.*, a.adj_factor
          FROM d
          LEFT JOIN a
          USING (ts_code, trade_date)
        ),
        w AS (
          SELECT *, max(adj_factor) OVER (PARTITION BY ts_code) AS latest_adj_factor
          FROM j
        )
        SELECT
          *
          ,CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                ELSE open * adj_factor / latest_adj_factor END AS qfq_open
          ,CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                ELSE high * adj_factor / latest_adj_factor END AS qfq_high
          ,CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                ELSE low * adj_factor / latest_adj_factor END AS qfq_low
          ,CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                ELSE close * adj_factor / latest_adj_factor END AS qfq_close
          ,CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                ELSE pre_close * adj_factor / latest_adj_factor END AS qfq_pre_close
          ,CASE WHEN adj_factor IS NULL THEN NULL ELSE open * adj_factor END AS hfq_open
          ,CASE WHEN adj_factor IS NULL THEN NULL ELSE high * adj_factor END AS hfq_high
          ,CASE WHEN adj_factor IS NULL THEN NULL ELSE low * adj_factor END AS hfq_low
          ,CASE WHEN adj_factor IS NULL THEN NULL ELSE close * adj_factor END AS hfq_close
          ,CASE WHEN adj_factor IS NULL THEN NULL ELSE pre_close * adj_factor END AS hfq_pre_close
        FROM w
        WHERE ts_code = ?
        ORDER BY trade_date
        """

        df = con.execute(sql, params).fetchdf()
        if how == "qfq":
            cols = [c for c in df.columns if not c.startswith("hfq_")]
            df = df.loc[:, cols]
        elif how == "hfq":
            cols = [c for c in df.columns if not c.startswith("qfq_")]
            df = df.loc[:, cols]

        if cache and self._cache_enabled:
            self._cache.set(cache_key, df.copy(), size_bytes=_estimate_df_bytes(df))
        return df

    # -----------------------------
    # Escape hatches
    # -----------------------------
    def sql(self, query: str, params: list[Any] | None = None) -> pd.DataFrame:
        con = self._connect()
        return con.execute(query, params or []).fetchdf()

    def read(
        self,
        dataset: str,
        *,
        where: dict[str, Any] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        columns: list[str] | None = None,
        limit: int | None = None,
        order_by: str | None = None,
        exchange: str | None = None,
        cache: bool = True,
    ) -> pd.DataFrame:
        ds = (dataset or "").strip()
        if not ds:
            raise ValueError("dataset is required")

        cache_key = self._cache_key(
            f"read:{ds}",
            where=where,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            limit=limit,
            order_by=order_by,
            exchange=exchange,
        )
        if cache and self._cache_enabled:
            hit = self._cache.get(cache_key)
            if hit is not None:
                return hit.copy()

        if ds in {"stock_basic", "stock_company"}:
            df = self._read_snapshot_dataset(ds)
            df = _apply_where(df, where)
            df = _apply_columns_limit_order(df, columns, limit, order_by)
        elif ds == "trade_cal":
            df = self._read_trade_cal(exchange=(exchange or self.exchange_default).strip(), cache=cache)
            df = _apply_where(df, where)
            df = _apply_columns_limit_order(df, columns, limit, order_by)
        elif ds in self._YEAR_WINDOW_DATASETS:
            df = self._read_year_window_dataset(ds, year=None, cache=cache)
            df = _apply_where(df, where)
            df = _apply_columns_limit_order(df, columns, limit, order_by)
        elif ds in self._TRADE_DATE_DATASETS:
            expr, params = self._dataset_relation_expr(ds, start_date=start_date, end_date=end_date, exchange=exchange)
            sql_cols = "*" if not columns else ", ".join(_quote_ident(c) for c in columns)
            sql = f"SELECT {sql_cols} FROM {expr}"
            p = list(params)
            if where:
                w_sql, w_params = _where_sql(where)
                sql += f" WHERE {w_sql}"
                p.extend(w_params)
            if order_by:
                sql += f" ORDER BY {_parse_order_by(order_by)}"
            if limit is not None:
                sql += f" LIMIT {int(limit)}"
            con = self._connect()
            df = con.execute(sql, p).fetchdf()
        else:
            raise ValueError(f"Unknown dataset: {ds}")

        if cache and self._cache_enabled:
            self._cache.set(cache_key, df.copy(), size_bytes=_estimate_df_bytes(df))
        return df

    # -----------------------------
    # Internals: connections
    # -----------------------------
    def _connect(self):
        import duckdb

        with self._lock:
            if self._con is None:
                if os.path.exists(self.duckdb_path):
                    self._con = duckdb.connect(self.duckdb_path, read_only=True)
                else:
                    self._con = duckdb.connect(":memory:")
            return self._con

    def _compute_data_version_hint(self) -> str:
        try:
            if os.path.exists(self.duckdb_path):
                return f"duckdb_mtime:{os.path.getmtime(self.duckdb_path)}"
        except OSError:
            pass
        return "no_duckdb"

    # -----------------------------
    # Internals: file path helpers
    # -----------------------------
    def _partition_path(self, dataset: str, trade_date: str) -> str:
        d = parse_yyyymmdd(trade_date)
        return os.path.join(
            self.parquet_dir,
            dataset,
            f"year={d.year:04d}",
            f"month={d.month:02d}",
            f"trade_date={trade_date}.parquet",
        )

    def _snapshot_path(self, dataset: str, *, exchange: str | None = None) -> str:
        if dataset in {"stock_basic", "stock_company"}:
            return os.path.join(self.parquet_dir, dataset, "latest.parquet")
        if dataset == "trade_cal":
            ex = (exchange or self.exchange_default).strip()
            return os.path.join(self.parquet_dir, "trade_cal", f"{ex}_latest.parquet")
        raise ValueError(f"Not a snapshot dataset: {dataset}")

    def _year_window_glob(self, dataset: str, *, year: int | None) -> str | list[str]:
        base = os.path.join(self.parquet_dir, dataset)
        if year is None:
            return os.path.join(base, "*.parquet")
        return os.path.join(base, f"year={int(year)}.parquet")

    # -----------------------------
    # Internals: dataset readers
    # -----------------------------
    def _read_snapshot_dataset(self, dataset: str) -> pd.DataFrame:
        path = self._snapshot_path(dataset)
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")
        con = self._connect()
        return con.execute("SELECT * FROM read_parquet(?, union_by_name=true)", [path]).fetchdf()

    def _read_trade_cal(self, *, exchange: str, cache: bool) -> pd.DataFrame:
        path = self._snapshot_path("trade_cal", exchange=exchange)
        if not os.path.exists(path):
            # Fallback: pick any *_latest.parquet and let caller filter/inspect.
            tc_dir = os.path.join(self.parquet_dir, "trade_cal")
            try_any = []
            if os.path.isdir(tc_dir):
                for name in os.listdir(tc_dir):
                    if name.endswith("_latest.parquet"):
                        try_any.append(os.path.join(tc_dir, name))
            if not try_any:
                raise FileNotFoundError(f"Trade calendar not found: expected {path}")
            path = sorted(try_any)[0]

        con = self._connect()
        df = con.execute("SELECT * FROM read_parquet(?, union_by_name=true)", [path]).fetchdf()
        # Ensure types are stable.
        if "cal_date" in df.columns:
            df["cal_date"] = df["cal_date"].astype(str)
        if "pretrade_date" in df.columns:
            df["pretrade_date"] = df["pretrade_date"].astype(str)
        return df

    def _read_year_window_dataset(self, dataset: str, *, year: int | None, cache: bool) -> pd.DataFrame:
        g = self._year_window_glob(dataset, year=year)
        con = self._connect()
        # DuckDB supports glob in read_parquet.
        return con.execute("SELECT * FROM read_parquet(?, union_by_name=true)", [g]).fetchdf()

    def _dataset_relation_expr(
        self,
        dataset: str,
        *,
        start_date: str | None,
        end_date: str | None,
        exchange: str | None,
    ) -> tuple[str, list[Any]]:
        """Return (relation_sql, params) for a dataset relation."""
        if dataset not in self._TRADE_DATE_DATASETS:
            raise ValueError(f"Not a trade-date dataset: {dataset}")

        # If no date range is provided, fall back to glob (can be large).
        if not start_date or not end_date:
            glob_path = os.path.join(self.parquet_dir, dataset, "**", "*.parquet")
            return "read_parquet(?, union_by_name=true)", [glob_path]

        # Prefer pruning by exact partitions, using trade calendar.
        try:
            dates = self.trading_days(start_date, end_date, exchange=exchange)
            files = [self._partition_path(dataset, d) for d in dates]
            files = [p for p in files if os.path.exists(p)]
            if not files:
                # Nothing available locally in this range.
                # Return a relation that yields 0 rows with correct schema (best-effort):
                glob_path = os.path.join(self.parquet_dir, dataset, "**", "*.parquet")
                return "read_parquet(?, union_by_name=true)", [glob_path]
            # DuckDB SQL can accept a VARCHAR[] for read_parquet.
            return "read_parquet(?, union_by_name=true)", [files]
        except Exception:
            glob_path = os.path.join(self.parquet_dir, dataset, "**", "*.parquet")
            return "read_parquet(?, union_by_name=true)", [glob_path]

    def _read_trade_date_dataset(
        self,
        dataset: str,
        *,
        ts_code: str,
        start_date: str | None,
        end_date: str | None,
        columns: list[str] | None,
        order_by: str | None,
        exchange: str | None,
        cache: bool,
    ) -> pd.DataFrame:
        cache_key = self._cache_key(
            f"ds:{dataset}",
            ts_code=ts_code,
            start_date=start_date,
            end_date=end_date,
            columns=columns,
            order_by=order_by,
            exchange=exchange,
        )
        if cache and self._cache_enabled:
            hit = self._cache.get(cache_key)
            if hit is not None:
                return hit.copy()

        expr, params = self._dataset_relation_expr(dataset, start_date=start_date, end_date=end_date, exchange=exchange)
        sql_cols = "*" if not columns else ", ".join(_quote_ident(c) for c in columns)
        sql = f"SELECT {sql_cols} FROM {expr} WHERE ts_code = ?"
        p = [*params, ts_code]
        if start_date is not None:
            sql += " AND trade_date >= ?"
            p.append(str(start_date))
        if end_date is not None:
            sql += " AND trade_date <= ?"
            p.append(str(end_date))
        if order_by:
            sql += f" ORDER BY {_quote_ident(order_by)}"

        con = self._connect()
        df = con.execute(sql, p).fetchdf()

        if cache and self._cache_enabled:
            self._cache.set(cache_key, df.copy(), size_bytes=_estimate_df_bytes(df))
        return df

    # -----------------------------
    # Internals: metadata caches
    # -----------------------------
    def _stock_basic_minimal(self) -> pd.DataFrame:
        if self._stock_basic_min is not None:
            return self._stock_basic_min
        path = self._snapshot_path("stock_basic")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing parquet file: {path}")
        con = self._connect()
        df = con.execute(
            """
            SELECT symbol, ts_code, list_date
            FROM read_parquet(?, union_by_name=true)
            """,
            [path],
        ).fetchdf()
        # Normalize types for stable lookups.
        df["symbol"] = df["symbol"].astype(str)
        df["ts_code"] = df["ts_code"].astype(str)
        if "list_date" in df.columns:
            df["list_date"] = df["list_date"].astype(str)
        self._stock_basic_min = df
        return df

    # -----------------------------
    # Internals: cache key
    # -----------------------------
    def _cache_key(self, prefix: str, **kwargs: Any) -> str:
        parts = [prefix, f"store={self.store_dir}", f"v={self._data_version_hint}"]
        for k in sorted(kwargs.keys()):
            v = kwargs[k]
            parts.append(f"{k}={_stable_repr(v)}")
        return "|".join(parts)


def _estimate_df_bytes(df: pd.DataFrame) -> int:
    try:
        return int(df.memory_usage(index=True, deep=True).sum())
    except Exception:
        return 0


def _stable_repr(v: Any) -> str:
    if v is None:
        return "None"
    if isinstance(v, (str, int, float, bool)):
        return repr(v)
    if isinstance(v, (list, tuple)):
        return "[" + ",".join(_stable_repr(x) for x in v) + "]"
    if isinstance(v, dict):
        items = ",".join(f"{_stable_repr(k)}:{_stable_repr(v[k])}" for k in sorted(v.keys()))
        return "{" + items + "}"
    return repr(v)


def _quote_ident(name: str) -> str:
    # DuckDB uses double quotes for identifiers.
    n = str(name).replace('"', '""')
    return f'"{n}"'


def _parse_order_by(order_by: str) -> str:
    """Parse order_by string to handle 'column_name desc' or 'column_name asc' patterns."""
    parts = order_by.strip().split()
    if len(parts) == 1:
        return _quote_ident(parts[0])
    elif len(parts) == 2:
        col, direction = parts
        direction_upper = direction.upper()
        if direction_upper in ("ASC", "DESC"):
            return f"{_quote_ident(col)} {direction_upper}"
    # Fallback: quote as-is (original behavior)
    return _quote_ident(order_by)


def _where_sql(where: dict[str, Any]) -> tuple[str, list[Any]]:
    parts: list[str] = []
    params: list[Any] = []
    for k in sorted(where.keys()):
        col = _quote_ident(k)
        v = where[k]
        if v is None:
            parts.append(f"{col} IS NULL")
        elif isinstance(v, (list, tuple, set)):
            vs = list(v)
            if not vs:
                parts.append("1=0")
            else:
                placeholders = ",".join(["?"] * len(vs))
                parts.append(f"{col} IN ({placeholders})")
                params.extend(vs)
        else:
            parts.append(f"{col} = ?")
            params.append(v)
    return " AND ".join(parts) if parts else "1=1", params


def _apply_where(df: pd.DataFrame, where: dict[str, Any] | None) -> pd.DataFrame:
    if not where:
        return df
    out = df
    for k in sorted(where.keys()):
        if k not in out.columns:
            continue
        v = where[k]
        if v is None:
            out = out.loc[out[k].isna()]
        elif isinstance(v, (list, tuple, set)):
            out = out.loc[out[k].isin(list(v))]
        else:
            out = out.loc[out[k] == v]
    return out


def _apply_columns_limit_order(
    df: pd.DataFrame,
    columns: list[str] | None,
    limit: int | None,
    order_by: str | None,
) -> pd.DataFrame:
    out = df
    if columns:
        cols = [c for c in columns if c in out.columns]
        out = out.loc[:, cols]
    if order_by:
        # Handle "column desc" or "column asc" patterns
        parts = order_by.strip().split()
        col = parts[0]
        ascending = True
        if len(parts) == 2 and parts[1].upper() == "DESC":
            ascending = False
        if col in out.columns:
            out = out.sort_values(col, ascending=ascending)
    if limit is not None:
        out = out.head(int(limit))
    return out

