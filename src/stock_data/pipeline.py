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
    from stock_data.jobs.derived import ensure_derived_views

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    cleaned = cat.fail_running_partitions()
    if cleaned and cleaned > 0:
        logger.warning("Marked %d stale running partitions as failed", cleaned)

    # Define dataset categories
    basic_datasets = {"stock_basic", "trade_cal", "stock_company", "new_share", "namechange"}
    finance_datasets = {"income", "balancesheet", "cashflow", "forecast", "express", "dividend", "fina_indicator", "fina_audit", "fina_mainbz", "disclosure_date"}
    market_datasets = [d for d in selected if d not in basic_datasets and d not in finance_datasets]

    # Basic first (universe + calendar).
    run_basic(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in basic_datasets], start_date=start_date, end_date=end_date)

    # Market (uses trade_cal and is date-partitioned).
    run_market(cfg, token=token, catalog=cat, datasets=market_datasets, start_date=start_date, end_date=end_date)

    # Finance (report-period partitioned).
    run_finance(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in finance_datasets], start_date=start_date, end_date=end_date)

    ensure_derived_views(cat)


def update(cfg: RunConfig, *, token: str, end_date: str, datasets: str) -> None:
    _ensure_dirs(cfg)
    selected = parse_datasets(datasets)

    from stock_data.storage.duckdb_catalog import DuckDBCatalog
    from stock_data.jobs.basic import run_basic
    from stock_data.jobs.market import run_market
    from stock_data.jobs.finance import run_finance
    from stock_data.jobs.derived import ensure_derived_views

    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()

    cleaned = cat.fail_running_partitions()
    if cleaned and cleaned > 0:
        logger.warning("Marked %d stale running partitions as failed", cleaned)

    # Define dataset categories
    basic_datasets = {"stock_basic", "trade_cal", "stock_company", "new_share", "namechange"}
    finance_datasets = {"income", "balancesheet", "cashflow", "forecast", "express", "dividend", "fina_indicator", "fina_audit", "fina_mainbz", "disclosure_date"}
    market_datasets = [d for d in selected if d not in basic_datasets and d not in finance_datasets]

    # Basic refresh (cheap snapshots).
    run_basic(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in basic_datasets], start_date=None, end_date=end_date)

    # Market incremental (figures out missing days from ingestion_state + trade_cal).
    run_market(cfg, token=token, catalog=cat, datasets=market_datasets, start_date=None, end_date=end_date)

    # Finance incremental (figures out missing quarters from ingestion_state).
    run_finance(cfg, token=token, catalog=cat, datasets=[d for d in selected if d in finance_datasets], start_date=None, end_date=end_date)

    ensure_derived_views(cat)

