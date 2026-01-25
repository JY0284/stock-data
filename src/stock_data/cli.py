from __future__ import annotations

import argparse
import os
import sys


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="stock-data", description="Tushare local data lake")
    sub = p.add_subparsers(dest="cmd", required=True)

    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--store", default="store", help="Base storage directory (default: store)")
    common.add_argument("--rpm", type=int, default=500, help="Max remote calls per minute (default: 500)")
    common.add_argument("--workers", type=int, default=12, help="Concurrent workers (default: 12)")

    backfill = sub.add_parser("backfill", parents=[common], help="Backfill historical datasets")
    backfill.add_argument("--start-date", required=True, help="YYYYMMDD")
    backfill.add_argument("--end-date", required=True, help="YYYYMMDD")
    backfill.add_argument(
        "--datasets",
        default="all",
        help="Comma-separated dataset names or 'all' (default: all)",
    )

    update = sub.add_parser("update", parents=[common], help="Incrementally update datasets")
    update.add_argument("--end-date", default=None, help="YYYYMMDD (default: today)")
    update.add_argument("--datasets", default="all", help="Comma-separated dataset names or 'all'")

    validate = sub.add_parser("validate", parents=[common], help="Validate stored datasets")
    validate.add_argument("--datasets", default="all", help="Comma-separated dataset names or 'all'")

    clean_store = sub.add_parser(
        "clean-store",
        parents=[common],
        help="Remove macOS metadata files (e.g. ._*.parquet) from the store",
    )
    clean_store.add_argument("--dry-run", action="store_true", help="Only print what would be deleted")

    query = sub.add_parser("query", parents=[common], help="Run a SQL query in DuckDB")
    query.add_argument("--sql", required=True, help="SQL string to execute")

    stat = sub.add_parser("stat", parents=[common], help="Show local data coverage and size statistics")
    stat.add_argument("--datasets", default="all", help="Comma-separated dataset names or 'all'")

    datasets = sub.add_parser("datasets", parents=[common], help="List supported datasets and descriptions")
    datasets.add_argument("--lang", default="both", choices=["both", "en", "zh"], help="Output language (default: both)")

    serve = sub.add_parser("serve", parents=[common], help="Start a read-only HTTP service for the local store")
    serve.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    serve.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    serve.add_argument("--http-workers", type=int, default=1, help="Uvicorn workers (default: 1)")
    serve.add_argument("--log-level", default=None, help="Override uvicorn log level (default: inherit)")

    sync = sub.add_parser("sync", parents=[common], help="Sync local store from a remote stock-data service")
    sync.add_argument("--remote-host", required=True, help="Remote service host (IP or DNS)")
    sync.add_argument("--remote-port", type=int, required=True, help="Remote service port")
    sync.add_argument("--remote-scheme", default="http", choices=["http", "https"], help="Remote scheme")
    sync.add_argument(
        "--delete",
        action="store_true",
        help="Delete local files that do not exist on remote (DANGEROUS)",
    )
    sync.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be changed",
    )
    sync.add_argument(
        "--hash",
        action="store_true",
        help="Verify sha256 for each file (slower, safer)",
    )
    sync.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Parallel download workers (default: 4)",
    )

    p.add_argument(
        "--token",
        default=None,
        help="Tushare token (default: env TUSHARE_TOKEN)",
    )

    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    # Import lazily so `stock-data --help` works without heavy deps installed.
    from stock_data.runner import run_command

    token = args.token or os.environ.get("TUSHARE_TOKEN")
    if args.cmd in {"backfill", "update"} and not token:
        print("Missing token: set env TUSHARE_TOKEN or pass --token", file=sys.stderr)
        return 2

    return run_command(args, token=token)


if __name__ == "__main__":
    raise SystemExit(main())

