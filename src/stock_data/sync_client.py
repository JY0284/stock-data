from __future__ import annotations

import concurrent.futures
import hashlib
import json
import os
import tempfile
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SyncResult:
    downloaded: int
    skipped: int
    deleted: int
    errors: list[str]


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _http_get_json(url: str, *, timeout: float = 60.0) -> dict:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
        raw = resp.read()
    return json.loads(raw.decode("utf-8"))


def _download_to_atomic(
    *,
    url: str,
    dest: Path,
    expected_size: int | None,
    expected_sha256: str | None,
    mtime_ns: int | None,
    timeout: float = 60.0,
) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp_fd, tmp_path = tempfile.mkstemp(prefix=dest.name + ".", suffix=".tmp", dir=str(dest.parent))
    tmp = Path(tmp_path)
    try:
        h = hashlib.sha256() if expected_sha256 else None
        written = 0
        with os.fdopen(tmp_fd, "wb") as out:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310
                while True:
                    chunk = resp.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
                    written += len(chunk)
                    if h is not None:
                        h.update(chunk)

        if expected_size is not None and written != int(expected_size):
            raise RuntimeError(f"size mismatch for {dest}: expected {expected_size}, got {written}")

        if expected_sha256 is not None:
            got = (h.hexdigest() if h is not None else _sha256_file(tmp))
            if got != expected_sha256:
                raise RuntimeError(f"sha256 mismatch for {dest}: expected {expected_sha256}, got {got}")

        os.replace(str(tmp), str(dest))
        if mtime_ns is not None:
            # Preserve remote mtime as best-effort.
            os.utime(str(dest), ns=(int(mtime_ns), int(mtime_ns)))
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            pass


def sync_store(
    *,
    base_url: str,
    store_dir: str,
    delete: bool = False,
    dry_run: bool = False,
    verify_hash: bool = False,
    concurrency: int = 4,
    show_progress: bool = False,
) -> SyncResult:
    """Sync local store from remote.

    Cross-platform notes:
    - Uses stdlib `urllib` for HTTP.
    - Uses atomic `os.replace` for Windows/Linux/macOS.
    """

    base = (base_url or "").rstrip("/")
    if not base.startswith("http://") and not base.startswith("https://"):
        raise ValueError("base_url must start with http:// or https://")

    local_root = Path(store_dir).expanduser().resolve()

    manifest_url = f"{base}/sync/manifest?hash={'true' if verify_hash else 'false'}"
    manifest = _http_get_json(manifest_url)
    files = manifest.get("files")
    if not isinstance(files, list):
        raise RuntimeError("remote manifest missing 'files'")

    progress = None
    progress_ctx = None
    scan_task_id = None
    download_task_id = None
    if show_progress:
        try:
            from rich.progress import (
                BarColumn,
                MofNCompleteColumn,
                Progress,
                SpinnerColumn,
                TaskProgressColumn,
                TextColumn,
                TimeElapsedColumn,
                TimeRemainingColumn,
            )

            progress = Progress(
                SpinnerColumn(),
                TextColumn("{task.description}"),
                BarColumn(),
                TaskProgressColumn(),
                MofNCompleteColumn(),
                TimeElapsedColumn(),
                TimeRemainingColumn(),
                transient=True,
            )
            progress_ctx = progress
        except Exception:
            progress = None
            progress_ctx = None

    # Build a set for optional deletion.
    remote_paths: set[str] = set()

    to_download: list[dict] = []
    skipped = 0

    if progress_ctx is not None:
        progress_ctx.__enter__()
        scan_task_id = progress.add_task("sync: scanning", total=len(files))

    try:
        # Batch progress updates to reduce overhead for large manifests.
        scan_pending = 0
        scan_flush_every = 200

        for item in files:
            scan_pending += 1
            if progress is not None and scan_task_id is not None and scan_pending >= scan_flush_every:
                progress.advance(scan_task_id, scan_pending)
                scan_pending = 0

            if not isinstance(item, dict):
                continue
            rel = item.get("path")
            if not isinstance(rel, str) or not rel.strip():
                continue

            # Remote always uses '/' separators.
            rel = rel.lstrip("/")
            remote_paths.add(rel)

            size = item.get("size")
            mtime_ns = item.get("mtime_ns")
            sha256 = item.get("sha256") if verify_hash else None

            dest = (local_root / rel).resolve()
            try:
                dest.relative_to(local_root)
            except Exception:
                # Path traversal / weird paths.
                continue

            # Decide if we need download.
            # Default behavior is intentionally *mtime-insensitive* because mtimes often differ
            # across machines/filesystems even when content is identical.
            if dest.exists() and dest.is_file():
                st = dest.stat()

                if verify_hash and sha256:
                    try:
                        if _sha256_file(dest) == sha256:
                            skipped += 1
                            continue
                    except Exception:
                        # If hashing fails, fall back to download.
                        pass
                else:
                    if size is not None and int(size) == int(st.st_size):
                        skipped += 1
                        continue

                    # Fallback only when remote didn't provide size.
                    if mtime_ns is not None:
                        local_mtime_ns = int(getattr(st, "st_mtime_ns", int(st.st_mtime * 1e9)))
                        if int(mtime_ns) == local_mtime_ns:
                            skipped += 1
                            continue

            to_download.append({"rel": rel, "size": size, "mtime_ns": mtime_ns, "sha256": sha256})

        if progress is not None and scan_task_id is not None and scan_pending:
            progress.advance(scan_task_id, scan_pending)
    finally:
        # Keep the progress context open for the download phase.
        pass

    errors: list[str] = []

    def _one(job: dict) -> None:
        rel = job["rel"]
        dest = (local_root / rel).resolve()
        file_url = f"{base}/sync/file?{urllib.parse.urlencode({'path': rel})}"
        if dry_run:
            return
        _download_to_atomic(
            url=file_url,
            dest=dest,
            expected_size=(int(job["size"]) if job.get("size") is not None else None),
            expected_sha256=(job.get("sha256") if verify_hash else None),
            mtime_ns=(int(job["mtime_ns"]) if job.get("mtime_ns") is not None else None),
        )

    downloaded = 0

    if to_download:
        max_workers = max(1, int(concurrency))

        if progress is not None:
            download_task_id = progress.add_task("sync: downloading", total=len(to_download))

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
            futs = [ex.submit(_one, j) for j in to_download]
            for fut in concurrent.futures.as_completed(futs):
                try:
                    fut.result()
                    downloaded += 1
                except Exception as e:  # noqa: BLE001
                    errors.append(str(e))
                finally:
                    if progress is not None and download_task_id is not None:
                        progress.advance(download_task_id, 1)

    deleted = 0
    if delete:
        # Only consider the managed subtrees.
        for rel_base in ("duckdb", "parquet"):
            base_path = (local_root / rel_base)
            if not base_path.exists():
                continue
            for p in base_path.rglob("*"):
                if not p.is_file():
                    continue
                try:
                    rel = p.resolve().relative_to(local_root).as_posix()
                except Exception:
                    continue
                if rel not in remote_paths:
                    if dry_run:
                        deleted += 1
                        continue
                    try:
                        p.unlink()
                        deleted += 1
                    except Exception as e:  # noqa: BLE001
                        errors.append(f"delete {rel}: {e}")

    if progress_ctx is not None:
        progress_ctx.__exit__(None, None, None)

    return SyncResult(downloaded=downloaded, skipped=skipped, deleted=deleted, errors=errors)
