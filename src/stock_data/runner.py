from __future__ import annotations

import datetime as _dt
import os
import logging
from dataclasses import dataclass


@dataclass(frozen=True)
class RunConfig:
    store_dir: str
    rpm: int
    workers: int

    @property
    def parquet_dir(self) -> str:
        return os.path.join(self.store_dir, "parquet")

    @property
    def duckdb_path(self) -> str:
        return os.path.join(self.store_dir, "duckdb", "market.duckdb")


def _today_yyyymmdd() -> str:
    return _dt.date.today().strftime("%Y%m%d")


def run_command(args, *, token: str) -> int:
    # Configure logging once for CLI execution.
    # Can be overridden via env: STOCK_DATA_LOG_LEVEL=DEBUG/INFO/WARNING
    level_name = os.environ.get("STOCK_DATA_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    try:
        from rich.logging import RichHandler

        logging.basicConfig(
            level=level,
            format="%(message)s",
            datefmt="[%X]",
            handlers=[RichHandler(rich_tracebacks=True, show_path=False)],
        )
    except Exception:  # noqa: BLE001
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )

    cfg = RunConfig(store_dir=args.store, rpm=args.rpm, workers=args.workers)

    if args.cmd == "clean-store":
        from stock_data.clean_store import clean_store_dir

        res = clean_store_dir(cfg.store_dir, dry_run=bool(getattr(args, "dry_run", False)))
        action = "would remove" if bool(getattr(args, "dry_run", False)) else "removed"
        print(f"clean-store: {action} {res.deleted_files} files, {res.deleted_dirs} dirs")
        return 0

    if args.cmd == "query":
        from stock_data.storage.duckdb_catalog import DuckDBCatalog

        cat = DuckDBCatalog(cfg.duckdb_path, cfg.parquet_dir)
        with cat.connect() as con:
            out = con.execute(args.sql).fetchdf()
        print(out.to_string(index=False))
        return 0

    if args.cmd == "stat":
        from stock_data.stats import print_stats

        print_stats(cfg, datasets=args.datasets)
        return 0

    if args.cmd == "datasets":
        from stock_data.datasets import print_datasets

        print_datasets(lang=args.lang)
        return 0

    if args.cmd == "serve":
        # Configure the web service via env vars so uvicorn can import a global app.
        # This enables multi-worker mode.
        os.environ["STOCK_DATA_STORE_DIR"] = cfg.store_dir

        # Prefer import-string when workers > 1 (uvicorn requirement).
        import uvicorn

        log_level = (args.log_level or os.environ.get("STOCK_DATA_WEB_LOG_LEVEL") or "info").lower()
        workers = int(getattr(args, "http_workers", 1) or 1)

        if workers > 1:
            uvicorn.run(
                "stock_data.web_service:app",
                host=args.host,
                port=int(args.port),
                workers=workers,
                log_level=log_level,
            )
        else:
            from stock_data.web_service import create_app

            app = create_app()
            uvicorn.run(app, host=args.host, port=int(args.port), log_level=log_level)

        return 0

    if args.cmd == "validate":
        from stock_data.validation import validate

        # Validation is primarily local. Only do optional remote spot-check when
        # the user explicitly provides --token.
        validate(cfg, token=getattr(args, "token", None) or None)
        return 0

    end_date = getattr(args, "end_date", None) or _today_yyyymmdd()

    if args.cmd == "backfill":
        start_date = args.start_date
        datasets = args.datasets
        from stock_data.pipeline import backfill

        backfill(cfg, token=token, start_date=start_date, end_date=end_date, datasets=datasets)
        return 0

    if args.cmd == "update":
        datasets = args.datasets
        from stock_data.pipeline import update

        update(cfg, token=token, end_date=end_date, datasets=datasets)
        return 0

    raise ValueError(f"Unknown command: {args.cmd}")

