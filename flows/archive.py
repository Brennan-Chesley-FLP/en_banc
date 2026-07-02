"""Archive-handler factory and run-DB archival: select the backend at runtime.

``ARCHIVE_BACKEND`` picks where scrape output is archived:

- ``s3`` (default) — stream file downloads to the ``files`` S3 bucket (SeaweedFS)
  and upload the run DB to the ``scrapes`` bucket.
- ``local`` — write to a local directory tree (an external drive bind-mounted
  into the container) rooted at ``ARCHIVE_LOCAL_ROOT``: file downloads via
  :class:`flows.local_archive.LocalFSArchiveHandler` and the finished run DB
  moved to ``{root}/{scraper_schema}/scrapes/{db_name}.db``. Used to take the
  object store out of the archive path when diagnosing throughput.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from flows.local_archive import LocalFSArchiveHandler
from flows.s3_archive import make_s3_archive_handler

_LOCAL_BACKENDS = {"local", "fs", "localfs"}


def is_local_backend() -> bool:
    """Whether ``ARCHIVE_BACKEND`` selects the local-filesystem backend."""
    return os.environ.get("ARCHIVE_BACKEND", "s3").strip().lower() in _LOCAL_BACKENDS


def _local_root() -> Path:
    root = os.environ.get("ARCHIVE_LOCAL_ROOT", "").strip()
    if not root:
        raise RuntimeError(
            "ARCHIVE_BACKEND=local requires ARCHIVE_LOCAL_ROOT to be set "
            "(the in-container archive root, e.g. /archive)."
        )
    return Path(root)


async def make_archive_handler(prefix: str):
    """Build the file-download archive handler selected by ``ARCHIVE_BACKEND``."""
    if is_local_backend():
        return LocalFSArchiveHandler(root=str(_local_root()), prefix=prefix)
    return await make_s3_archive_handler(prefix=prefix)


def move_db_to_archive(db_path: Path, scraper_schema: str) -> str:
    """Move a finished run DB into the local archive; return its ``file://`` URL.

    Destination is ``{ARCHIVE_LOCAL_ROOT}/{scraper_schema}/scrapes/{db_name}.db``.
    The DB lives on the worker's runs volume (internal disk) while the archive is
    typically a different filesystem (external drive), so this copies to a
    ``.partial`` staged on the destination filesystem, atomically renames it into
    place, verifies the size, and only then unlinks the source — a crash never
    leaves a truncated ``.db`` at the final path, and a failed copy leaves the
    source intact for a retry.

    Synchronous (filesystem I/O); call via ``asyncio.to_thread`` from the flow.
    """
    dest = _local_root() / scraper_schema / "scrapes" / db_path.name
    dest.parent.mkdir(parents=True, exist_ok=True)

    src_size = db_path.stat().st_size
    staged = dest.with_name(dest.name + ".partial")
    shutil.copyfile(str(db_path), str(staged))
    os.replace(str(staged), str(dest))  # atomic within the archive filesystem

    dest_size = dest.stat().st_size
    if dest_size != src_size:
        raise RuntimeError(
            f"Archived DB size mismatch at {dest}: "
            f"source={src_size} bytes, archived={dest_size} bytes"
        )

    db_path.unlink()  # the "move" — only after a verified copy
    return f"file://{dest}"
