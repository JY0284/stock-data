from __future__ import annotations

import glob
import logging
import os
from collections.abc import Iterable


logger = logging.getLogger(__name__)


def _has_any_parquet(glob_path: str) -> bool:
    return bool(glob.glob(glob_path, recursive=True))


def _parquet_glob(parquet_root: str, dataset: str) -> str:
    # DuckDB read_parquet supports globbing.
    # Exclude macOS AppleDouble dotfiles (e.g. `._*.parquet`) which can appear
    # after zip/unzip and break DuckDB parquet readers.
    return os.path.join(parquet_root, dataset, "**", "[!.]*.parquet")


def register_parquet_views(
    con,
    *,
    parquet_root: str,
    view_prefix: str = "v_",
    also_create_dataset_views: bool = True,
    datasets: Iterable[str] | None = None,
) -> dict[str, bool]:
    """Register parquet-backed views in a DuckDB connection.

    This is designed for two scenarios:
    - writer metadata DB: persistent views created in on-disk DuckDB
    - reader/query: ephemeral views created in an in-memory DuckDB

    Returns a mapping {dataset: exists}.
    """

    # Trade-date partitioned datasets in this repo.
    base_datasets = [
        "daily",
        "adj_factor",
        "daily_basic",
        "stk_limit",
        "suspend_d",
        "weekly",
        "monthly",
        "etf_daily",
        # US stock daily bars.
        "us_daily",
        # US stock indices (derived/calculated).
        "us_index_broad",
        "us_index_nasdaq",
        "us_index_sp500",
        "us_index_ndx100",
        "us_index_djia30",
    ]

    # Finance datasets (report-period partitioned)
    finance_datasets = [
        "income",
        "balancesheet",
        "cashflow",
        "forecast",
        "express",
        "fina_indicator",
        "fina_mainbz",
    ]

    # Snapshots written as single files under <dataset>/<name>.parquet
    snapshot_datasets = [
        "stock_basic",
        "trade_cal",
        "stock_company",
        "index_basic",
        "fund_basic",
        "disclosure_date",
        # Writer metadata exported as parquet snapshot.
        "ingestion_state",
        # Macro snapshots.
        "lpr",
        "cpi",
        "cn_sf",
        "cn_m",
        # US stock snapshots.
        "us_basic",
        "us_tradecal",
    ]

    # Windowed snapshot datasets (one file per year).
    windowed_snapshot_datasets = [
        "new_share",
        "namechange",
    ]

    # ts_code-partitioned datasets (one parquet per code).
    ts_code_datasets = [
        "index_daily",
        "dividend",
        "fina_audit",
        "fund_nav",
        "fund_share",
        "fund_div",
    ]

    if datasets is None:
        all_datasets = (
            base_datasets
            + finance_datasets
            + snapshot_datasets
            + windowed_snapshot_datasets
            + ts_code_datasets
        )
    else:
        all_datasets = list(datasets)

    exists: dict[str, bool] = {}
    globs: dict[str, str] = {}
    for ds in all_datasets:
        g = _parquet_glob(parquet_root, ds)
        globs[ds] = g
        exists[ds] = _has_any_parquet(g)

    for ds in all_datasets:
        if not exists.get(ds):
            continue
        view_name = f"{view_prefix}{ds}" if view_prefix else ds
        con.execute(
            f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{globs[ds]}', union_by_name=true);"
        )
        if also_create_dataset_views and view_name != ds:
            con.execute(f"CREATE OR REPLACE VIEW {ds} AS SELECT * FROM {view_name};")

    # Derived qfq/hfq view (v_daily_adj). Best-effort only.
    if exists.get("daily") and exists.get("adj_factor"):
        try:
            v_daily = f"{view_prefix}daily" if view_prefix else "daily"
            v_adj = f"{view_prefix}adj_factor" if view_prefix else "adj_factor"
            v_out = f"{view_prefix}daily_adj" if view_prefix else "daily_adj"

            daily_cols = {r[0] for r in con.execute(f"SELECT name FROM pragma_table_info('{v_daily}')").fetchall()}
            adj_cols = {r[0] for r in con.execute(f"SELECT name FROM pragma_table_info('{v_adj}')").fetchall()}

            required_daily = {"ts_code", "trade_date", "open", "high", "low", "close", "pre_close"}
            required_adj = {"ts_code", "trade_date", "adj_factor"}

            if not (required_daily <= daily_cols):
                logger.warning("skip %s: %s missing columns: %s", v_out, v_daily, sorted(required_daily - daily_cols))
                return exists
            if not (required_adj <= adj_cols):
                logger.warning("skip %s: %s missing columns: %s", v_out, v_adj, sorted(required_adj - adj_cols))
                return exists

            con.execute(
                f"""
                CREATE OR REPLACE VIEW {v_out} AS
                WITH j AS (
                  SELECT
                    d.*,
                    a.adj_factor
                  FROM {v_daily} d
                  LEFT JOIN {v_adj} a
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
            logger.exception("skip derived daily_adj view due to error")

    return exists
