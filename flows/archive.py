"""Archive-handler factory: select the storage backend at runtime.

``ARCHIVE_BACKEND`` picks where JKent file downloads are archived:

- ``s3`` (default) — stream to the ``files`` S3 bucket (SeaweedFS) via
  :func:`flows.s3_archive.make_s3_archive_handler`.
- ``local`` — write to a local directory tree (an external drive bind-mounted
  into the container) via :class:`flows.local_archive.LocalFSArchiveHandler`,
  rooted at ``ARCHIVE_LOCAL_ROOT``. Used to take the object store out of the
  archive write path when diagnosing throughput.
"""

from __future__ import annotations

import os

from flows.local_archive import LocalFSArchiveHandler
from flows.s3_archive import make_s3_archive_handler

_LOCAL_BACKENDS = {"local", "fs", "localfs"}


async def make_archive_handler(prefix: str):
    """Build the archive handler selected by ``ARCHIVE_BACKEND`` (default s3)."""
    backend = os.environ.get("ARCHIVE_BACKEND", "s3").strip().lower()
    if backend in _LOCAL_BACKENDS:
        root = os.environ.get("ARCHIVE_LOCAL_ROOT", "").strip()
        if not root:
            raise RuntimeError(
                "ARCHIVE_BACKEND=local requires ARCHIVE_LOCAL_ROOT to be set "
                "(the in-container archive root, e.g. /archive)."
            )
        return LocalFSArchiveHandler(root=root, prefix=prefix)
    return await make_s3_archive_handler(prefix=prefix)
