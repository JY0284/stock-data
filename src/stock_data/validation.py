from __future__ import annotations

import logging
import os
from dataclasses import dataclass

from stock_data.jobs.derived import ensure_derived_views
from stock_data.rate_limit import RateLimiter
from stock_data.retry import RateLimitError, TransientError
from stock_data.runner import RunConfig
from stock_data.storage.duckdb_catalog import DuckDBCatalog
from stock_data.tushare_client import TushareClient


logger = logging.getLogger(__name__)


def _table_exists(con, name: str) -> bool:
    row = con.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?;", [name]).fetchone()
    return bool(row and row[0] > 0)


def _view_exists(con, name: str) -> bool:
    row = con.execute("SELECT COUNT(*) FROM information_schema.views WHERE table_name = ?;", [name]).fetchone()
    return bool(row and row[0] > 0)


def validate(cfg: RunConfig, token: str | None = None) -> None:
    logger.info("validate: start")
    cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
    cat.ensure_schema()
    ensure_derived_views(cat)

    with cat.connect() as con:
        # 1) Key uniqueness checks
        logger.info("validate: uniqueness checks")
        for view in ["v_daily", "v_adj_factor", "v_daily_basic"]:
            if not _view_exists(con, view):
                continue
            dup = con.execute(
                f"""
                SELECT ts_code, trade_date, COUNT(*) AS c
                FROM {view}
                GROUP BY 1,2
                HAVING c > 1
                LIMIT 5;
                """
            ).fetchall()
            if dup:
                raise RuntimeError(f"Found duplicates in {view} (ts_code, trade_date). Example: {dup[0]}")

        # 2) Recent-day sanity: daily rowcount should be “not tiny”
        if _view_exists(con, "v_daily"):
            logger.info("validate: recent-day sanity")
            row = con.execute("SELECT MAX(trade_date) FROM v_daily;").fetchone()
            latest = row[0] if row else None
            if latest:
                cnt = con.execute("SELECT COUNT(*) FROM v_daily WHERE trade_date = ?;", [latest]).fetchone()[0]
                if cnt < 1000:
                    raise RuntimeError(f"daily rowcount too small for {latest}: {cnt}")

        # 3) Derived view exists when inputs exist
        if _view_exists(con, "v_daily") and _view_exists(con, "v_adj_factor"):
            logger.info("validate: derived view presence")
            if not _view_exists(con, "v_daily_adj"):
                raise RuntimeError("Expected v_daily_adj view to exist")

    # 4) Optional spot-check vs pro_bar (qfq) on latest range
    if not token:
        logger.info("validate: done (no remote spot-check)")
        return

    limiter = RateLimiter(rpm=min(cfg.rpm, 120))  # keep validation gentle
    client = TushareClient(token=token, limiter=limiter)

    with cat.connect() as con:
        if not _view_exists(con, "v_daily_adj"):
            logger.info("validate: done (no v_daily_adj)")
            return
        latest = con.execute("SELECT MAX(trade_date) FROM v_daily;").fetchone()[0]
        if not latest:
            logger.info("validate: done (no v_daily)")
            return
        # Pick a few liquid-looking symbols (first 3) from stock_basic if present, else from daily.
        ts_codes = []
        stock_basic_path = os.path.join(cfg.parquet_dir, "stock_basic", "latest.parquet")
        if os.path.exists(stock_basic_path):
            ts_codes = [r[0] for r in con.execute(f"SELECT ts_code FROM read_parquet('{stock_basic_path}') LIMIT 3;").fetchall()]
        if not ts_codes:
            ts_codes = [r[0] for r in con.execute("SELECT DISTINCT ts_code FROM v_daily LIMIT 3;").fetchall()]

        # Compare last ~30 trading days.
        start = con.execute(
            """
            SELECT MIN(trade_date) FROM (
              SELECT DISTINCT trade_date
              FROM v_daily
              WHERE trade_date <= ?
              ORDER BY trade_date DESC
              LIMIT 30
            );
            """,
            [latest],
        ).fetchone()[0]
        if not start:
            logger.info("validate: done (not enough days)")
            return

        logger.info("validate: remote spot-check (%d symbols)", len(ts_codes))
        for ts_code in ts_codes:
            local = con.execute(
                """
                SELECT trade_date, qfq_close
                FROM v_daily_adj
                WHERE ts_code = ? AND trade_date BETWEEN ? AND ?
                ORDER BY trade_date;
                """,
                [ts_code, start, latest],
            ).fetchdf()

            try:
                remote = client.pro_bar(ts_code=ts_code, adj="qfq", start_date=start, end_date=latest)
            except Exception as e:  # noqa: BLE001
                # Optional check: don't fail validation if upstream is throttling/flaky
                # or if the SDK itself errors (we've seen `OSError: ERROR.` from pro_bar).
                if isinstance(e, (RateLimitError, TransientError)):
                    logger.warning("validate: skip remote spot-check for %s due to %s", ts_code, e)
                else:
                    logger.warning("validate: skip remote spot-check for %s due to %r", ts_code, e)
                continue
            if remote is None or remote.empty or local.empty:
                continue
            remote = remote[["trade_date", "close"]].rename(columns={"close": "qfq_close_remote"})
            merged = local.merge(remote, on="trade_date", how="inner")
            if merged.empty:
                continue
            # allow tiny floating rounding error
            max_abs = (merged["qfq_close"] - merged["qfq_close_remote"]).abs().max()
            if max_abs and max_abs > 1e-4:
                raise RuntimeError(f"qfq spot-check failed for {ts_code}: max_abs_diff={max_abs}")

    logger.info("validate: done")


