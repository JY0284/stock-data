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
        import duckdb

        from stock_data.storage.parquet_views import register_parquet_views

        # Query must be non-conflicting with writers: never open the on-disk DuckDB.
        con = duckdb.connect(":memory:")
        try:
            register_parquet_views(con, parquet_root=cfg.parquet_dir, also_create_dataset_views=True)
            out = con.execute(args.sql).fetchdf()
        finally:
            con.close()
        print(out.to_string(index=False))
        return 0

    if args.cmd == "stat":
        from stock_data.stats import print_stats, write_stat_json_file

        output_path = getattr(args, "output", None)
        if output_path:
            write_stat_json_file(cfg, output_path, datasets=args.datasets)
            print(f"Wrote stat JSON to {output_path}")
        else:
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

    if args.cmd == "sync":
        from stock_data.sync_client import sync_store
        from urllib.parse import urlparse

        remote = (getattr(args, "remote", None) or "").strip()
        if remote:
            # Accept either full URL (recommended) or host:port.
            if "://" not in remote:
                remote = f"http://{remote}"
            u = urlparse(remote)
            if u.scheme not in {"http", "https"}:
                raise ValueError("--remote must be an http(s) URL, e.g. http://1.2.3.4:8000")
            if not u.hostname:
                raise ValueError("--remote must include a hostname, e.g. http://example.com")
            port = u.port
            if port is None:
                port = 80 if u.scheme == "http" else 443
            base_url = f"{u.scheme}://{u.hostname}:{int(port)}"
        else:
            if not getattr(args, "remote_host", None) or not getattr(args, "remote_port", None):
                raise ValueError("Missing remote: use --remote http://host:port or pass --remote-host/--remote-port")
            base_url = f"{args.remote_scheme}://{args.remote_host}:{int(args.remote_port)}"
        res = sync_store(
            base_url=base_url,
            store_dir=cfg.store_dir,
            delete=bool(getattr(args, "delete", False)),
            dry_run=bool(getattr(args, "dry_run", False)),
            verify_hash=bool(getattr(args, "hash", False)),
            concurrency=int(getattr(args, "concurrency", 4) or 4),
            show_progress=True,
        )

        if res.errors:
            print(f"sync: downloaded={res.downloaded} skipped={res.skipped} deleted={res.deleted} errors={len(res.errors)}")
            for e in res.errors[:20]:
                print(f"- {e}")
            return 1

        print(f"sync: downloaded={res.downloaded} skipped={res.skipped} deleted={res.deleted}")
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

