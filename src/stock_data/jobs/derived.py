from __future__ import annotations

import glob
import os
import logging

from stock_data.storage.duckdb_catalog import DuckDBCatalog


logger = logging.getLogger(__name__)


def _has_any_parquet(glob_path: str) -> bool:
    return bool(glob.glob(glob_path, recursive=True))


def ensure_derived_views(catalog: DuckDBCatalog) -> None:
    """
    Creates convenience DuckDB views over parquet datasets, including qfq/hfq derived prices.
    Safe to call repeatedly.
    """
    # Convenience base views (created if parquet exists).
    base_datasets = [
        "daily",
        "adj_factor",
        "daily_basic",
        "stk_limit",
        "suspend_d",
        "weekly",
        "monthly",
    ]
    
    # Finance datasets (report-period partitioned)
    finance_datasets = [
        "income",
        "balancesheet",
        "cashflow",
        "forecast",
        "express",
        "dividend",
        "fina_indicator",
        "fina_audit",
        "fina_mainbz",
    ]

    exists: dict[str, bool] = {}
    globs: dict[str, str] = {}
    for ds in base_datasets + finance_datasets:
        g = catalog.parquet_glob(ds)
        globs[ds] = g
        exists[ds] = _has_any_parquet(g)

    if not (exists["daily"] and exists["adj_factor"]):
        return

    with catalog.connect() as con:
        # 1) Base parquet-backed views. These should never make the pipeline fail.
        for ds in base_datasets:
            if not exists[ds]:
                continue
            con.execute(
                f"CREATE OR REPLACE VIEW v_{ds} AS SELECT * FROM read_parquet('{globs[ds]}', union_by_name=true);"
            )
        
        # 1b) Finance parquet-backed views
        for ds in finance_datasets:
            if not exists[ds]:
                continue
            con.execute(
                f"CREATE OR REPLACE VIEW v_{ds} AS SELECT * FROM read_parquet('{globs[ds]}', union_by_name=true);"
            )

        # 2) Derived qfq/hfq view. This is a convenience; skip if schema isn't ready.
        try:
            daily_cols = {r[0] for r in con.execute("SELECT name FROM pragma_table_info('v_daily')").fetchall()}
            adj_cols = {r[0] for r in con.execute("SELECT name FROM pragma_table_info('v_adj_factor')").fetchall()}

            required_daily = {"ts_code", "trade_date", "open", "high", "low", "close", "pre_close"}
            required_adj = {"ts_code", "trade_date", "adj_factor"}

            if not (required_daily <= daily_cols):
                logger.warning("skip v_daily_adj: v_daily missing columns: %s", sorted(required_daily - daily_cols))
                return
            if not (required_adj <= adj_cols):
                logger.warning("skip v_daily_adj: v_adj_factor missing columns: %s", sorted(required_adj - adj_cols))
                return

            con.execute(
                """
                CREATE OR REPLACE VIEW v_daily_adj AS
                WITH j AS (
                  SELECT
                    d.*,
                    a.adj_factor
                  FROM v_daily d
                  LEFT JOIN v_adj_factor a
                  USING (ts_code, trade_date)
                ),
                w AS (
                  SELECT
                    *,
                    max(adj_factor) OVER (PARTITION BY ts_code) AS latest_adj_factor
                  FROM j
                )
                SELECT
                  *,
                  CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                       ELSE open * adj_factor / latest_adj_factor END AS qfq_open,
                  CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                       ELSE high * adj_factor / latest_adj_factor END AS qfq_high,
                  CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                       ELSE low * adj_factor / latest_adj_factor END AS qfq_low,
                  CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                       ELSE close * adj_factor / latest_adj_factor END AS qfq_close,
                  CASE WHEN adj_factor IS NULL OR latest_adj_factor IS NULL OR latest_adj_factor = 0 THEN NULL
                       ELSE pre_close * adj_factor / latest_adj_factor END AS qfq_pre_close,

                  CASE WHEN adj_factor IS NULL THEN NULL ELSE open * adj_factor END AS hfq_open,
                  CASE WHEN adj_factor IS NULL THEN NULL ELSE high * adj_factor END AS hfq_high,
                  CASE WHEN adj_factor IS NULL THEN NULL ELSE low * adj_factor END AS hfq_low,
                  CASE WHEN adj_factor IS NULL THEN NULL ELSE close * adj_factor END AS hfq_close,
                  CASE WHEN adj_factor IS NULL THEN NULL ELSE pre_close * adj_factor END AS hfq_pre_close
                FROM w;
                """
            )
        except Exception:
            logger.exception("skip v_daily_adj due to error")
            return

