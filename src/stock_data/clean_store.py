from __future__ import annotations

import logging
import os
import shutil
from dataclasses import dataclass


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CleanStoreResult:
    deleted_files: int
    deleted_dirs: int


def _should_delete_file(name: str) -> bool:
    return name == ".DS_Store" or name.startswith("._")


def clean_store_dir(store_dir: str, *, dry_run: bool = False) -> CleanStoreResult:
    """Remove macOS metadata files accidentally included in archives.

    This targets AppleDouble files (e.g. `._*.parquet`) and `.DS_Store`, which can
    break DuckDB parquet globs after zip/unzip.

    Args:
        store_dir: Root `store/` directory.
        dry_run: If True, only logs what would be deleted.

    Returns:
        Counts of deleted files/dirs.
    """
    store_dir = os.path.abspath(store_dir)
    if not os.path.isdir(store_dir):
        raise FileNotFoundError(f"store dir not found: {store_dir}")

    deleted_files = 0
    deleted_dirs = 0

    # First remove macOS zip folder if present.
    macosx_dir = os.path.join(store_dir, "__MACOSX")
    if os.path.exists(macosx_dir):
        if dry_run:
            logger.info("[dry-run] remove dir: %s", macosx_dir)
        else:
            shutil.rmtree(macosx_dir, ignore_errors=True)
        deleted_dirs += 1

    for root, dirs, files in os.walk(store_dir):
        # Avoid descending into __MACOSX if it wasn't removed (e.g. permissions).
        if "__MACOSX" in dirs:
            dirs.remove("__MACOSX")

        for filename in files:
            if not _should_delete_file(filename):
                continue
            path = os.path.join(root, filename)
            if dry_run:
                logger.info("[dry-run] delete file: %s", path)
            else:
                try:
                    os.remove(path)
                except FileNotFoundError:
                    continue
            deleted_files += 1

    return CleanStoreResult(deleted_files=deleted_files, deleted_dirs=deleted_dirs)
