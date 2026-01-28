from __future__ import annotations

import logging
import os

from stock_data.runner import RunConfig
from stock_data.datasets import ALL_DATASET_NAMES, parse_datasets


logger = logging.getLogger(__name__)


ALL_DATASETS = ALL_DATASET_NAMES


def _ensure_dirs(cfg: RunConfig) -> None:
    os.makedirs(cfg.parquet_dir, exist_ok=True)
    os.makedirs(os.path.dirname(cfg.duckdb_path), exist_ok=True)


def backfill(cfg: RunConfig, *, token: str, start_date: str, end_date: str, datasets: str) -> None:
    _ensure_dirs(cfg)
    selected = parse_datasets(datasets)

    from stock_data.storage.duckdb_catalog import DuckDBCatalog
    from stock_data.jobs.basic import run_basic
    from stock_data.jobs.market import run_market
    from stock_data.jobs.finance import run_finance
    from stock_data.jobs.etf import run_etf
    from stock_data.jobs.derived import ensure_derived_views

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    cleaned = cat.fail_running_partitions()
    if cleaned and cleaned > 0:
        logger.warning("Marked %d stale running partitions as failed", cleaned)

    # Define dataset categories
    basic_datasets = {"stock_basic", "trade_cal", "stock_company", "index_basic", "fund_basic", "new_share", "namechange"}
    finance_datasets = {"income", "balancesheet", "cashflow", "forecast", "express", "dividend", "fina_indicator", "fina_audit", "fina_mainbz", "disclosure_date"}
    etf_datasets = {"fund_nav", "fund_share", "fund_div"}
    market_datasets = [d for d in selected if d not in basic_datasets and d not in finance_datasets and d not in etf_datasets]

    # Basic first (universe + calendar).
    run_basic(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in basic_datasets], start_date=start_date, end_date=end_date)

    # Market (uses trade_cal and is date-partitioned).
    run_market(cfg, token=token, catalog=cat, datasets=market_datasets, start_date=start_date, end_date=end_date)

    # Finance (report-period partitioned).
    run_finance(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in finance_datasets], start_date=start_date, end_date=end_date)

    # ETF (ts_code partitioned).
    run_etf(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in etf_datasets])

    ensure_derived_views(cat)


def update(cfg: RunConfig, *, token: str, end_date: str, datasets: str) -> None:
    _ensure_dirs(cfg)
    selected = parse_datasets(datasets)

    from stock_data.storage.duckdb_catalog import DuckDBCatalog
    from stock_data.jobs.basic import run_basic
    from stock_data.jobs.market import run_market
    from stock_data.jobs.finance import run_finance
    from stock_data.jobs.etf import run_etf
    from stock_data.jobs.derived import ensure_derived_views

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    cleaned = cat.fail_running_partitions()
    if cleaned and cleaned > 0:
        logger.warning("Marked %d stale running partitions as failed", cleaned)

    # Define dataset categories
    basic_datasets = {"stock_basic", "trade_cal", "stock_company", "index_basic", "fund_basic", "new_share", "namechange"}
    finance_datasets = {"income", "balancesheet", "cashflow", "forecast", "express", "dividend", "fina_indicator", "fina_audit", "fina_mainbz", "disclosure_date"}
    etf_datasets = {"fund_nav", "fund_share", "fund_div"}
    market_datasets = [d for d in selected if d not in basic_datasets and d not in finance_datasets and d not in etf_datasets]

    # Basic refresh (cheap snapshots).
    run_basic(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in basic_datasets], start_date=None, end_date=end_date)

    # Refresh policy for ts_code-partitioned datasets (ETF, dividend, fina_audit, index_daily).
    # These endpoints return full history for a code, but data keeps growing; without periodic
    # refresh, they become "complete once and never update".
    refresh_days: int | None = 7
    try:
        v = os.environ.get("STOCK_DATA_TS_CODE_REFRESH_DAYS")
        if v is not None and str(v).strip() != "":
            refresh_days = int(v)
    except Exception:
        refresh_days = 7

    if refresh_days is not None and int(refresh_days) <= 0:
        refresh_days = None

    # Market incremental (figures out missing days from ingestion_state + trade_cal).
    run_market(
        cfg,
        token=token,
        catalog=cat,
        datasets=market_datasets,
        start_date=None,
        end_date=end_date,
        index_daily_refresh_days=refresh_days,
    )

    # Finance incremental (figures out missing quarters from ingestion_state).
    run_finance(
        cfg,
        token=token,
        catalog=cat,
        datasets=[d for d in selected if d in finance_datasets],
        start_date=None,
        end_date=end_date,
        ts_code_refresh_days=refresh_days,
    )

    # ETF (ts_code partitioned).
    run_etf(
        cfg,
        token=token,
        catalog=cat,
        datasets=[d for d in selected if d in etf_datasets],
        refresh_days=refresh_days,
    )

    ensure_derived_views(cat)

