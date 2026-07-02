"""Local-filesystem archive handler — a SeaweedFS/S3 bypass for benchmarking.

Mirrors :class:`~flows.s3_archive.S3AsyncStreamingArchiveHandler`'s
content-addressed key layout but writes under a local root (typically an
external drive bind-mounted into the container) instead of putting to S3. This
takes the object store out of the archive write path so we can confirm whether
S3 ``PutObject`` is the throughput ceiling — the same ``archive.lookup`` /
``archive.download`` / ``archive.upload`` phases are emitted, so the existing
dashboards compare the two backends directly.

The stored path is the S3 key with the root prepended:
``{root}/{prefix}{sha[:2]}/{sha[2:4]}/{sha}{ext}``. Writes are staged to a
sibling ``.incoming`` dir on the *same* filesystem and atomically ``os.replace``d
into place, so a crash never leaves a half-written object at a content key.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import tempfile
from pathlib import Path
from typing import AsyncIterator

from jkent.data_types import ArchiveDecision
from jkent.observability import phase

from flows.s3_archive import _extension

logger = logging.getLogger(__name__)


class LocalFSArchiveHandler:
    """Streams JKent file downloads to a local directory tree.

    Args:
        root: Filesystem root the S3 key layout is written under (e.g.
            ``/archive`` inside the container, bind-mounted from the drive).
        prefix: Key prefix for stored files, e.g. ``"ny_nycourts/"``.
    """

    def __init__(self, root: str, prefix: str = "") -> None:
        self._root = Path(root)
        self._prefix = prefix
        # Stage writes on the SAME filesystem as the final location so the
        # rename is atomic — os.replace across devices raises OSError, and a
        # temp file under the system /tmp (internal disk) would force a slow
        # cross-device copy onto the external drive.
        self._tmp_dir = self._root / ".incoming"
        self._tmp_dir.mkdir(parents=True, exist_ok=True)

    # -- key helpers (identical layout to the S3 handler) ------------------

    def _content_key(self, sha256: str, ext: str) -> str:
        return f"{self._prefix}{sha256[:2]}/{sha256[2:4]}/{sha256}{ext}"

    def _dedup_pointer_key(self, deduplication_key: str) -> str:
        digest = hashlib.sha256(deduplication_key.encode()).hexdigest()
        return f"{self._prefix}_dedup/{digest}"

    def _path(self, key: str) -> Path:
        return self._root / key

    def _file_url(self, key: str) -> str:
        return f"file://{self._path(key)}"

    # -- sync helpers run via asyncio.to_thread ----------------------------

    def _lookup_dedup(self, deduplication_key: str) -> str | None:
        try:
            return self._path(self._dedup_pointer_key(deduplication_key)).read_text(
                "utf-8"
            )
        except FileNotFoundError:
            return None

    def _write(
        self,
        tmp_path: str,
        content_key: str,
        deduplication_key: str | None,
    ) -> str:
        final = self._path(content_key)
        if final.exists():
            # Content-addressed: identical bytes already stored — drop the temp.
            os.unlink(tmp_path)
        else:
            final.parent.mkdir(parents=True, exist_ok=True)
            os.replace(tmp_path, final)  # atomic within the same filesystem
        file_url = self._file_url(content_key)
        if deduplication_key:
            pointer = self._path(self._dedup_pointer_key(deduplication_key))
            pointer.parent.mkdir(parents=True, exist_ok=True)
            tmp_pointer = pointer.with_name(pointer.name + ".tmp")
            tmp_pointer.write_text(file_url, "utf-8")
            os.replace(tmp_pointer, pointer)
        return file_url

    # -- AsyncStreamingArchiveHandler protocol -----------------------------

    async def should_download(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
    ) -> ArchiveDecision:
        if deduplication_key:
            with phase("archive.lookup"):
                existing = await asyncio.to_thread(
                    self._lookup_dedup, deduplication_key
                )
            if existing is not None:
                logger.info("Skipping download, already archived: %s", url)
                return ArchiveDecision(download=False, file_url=existing)
        return ArchiveDecision(download=True)

    async def save_stream(
        self,
        url: str,
        deduplication_key: str | None,
        expected_type: str | None,
        hash_header_value: str | None,
        chunks: AsyncIterator[bytes],
    ) -> str:
        sha = hashlib.sha256()
        tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
            delete=False, dir=self._tmp_dir, prefix=".fs-stream-", suffix=".tmp"
        )
        try:
            with tmp as f, phase("archive.download"):
                async for chunk in chunks:
                    sha.update(chunk)
                    await asyncio.to_thread(f.write, chunk)
            content_key = self._content_key(
                sha.hexdigest(), _extension(url, expected_type)
            )
            with phase("archive.upload"):
                file_url = await asyncio.to_thread(
                    self._write, tmp.name, content_key, deduplication_key
                )
        finally:
            Path(tmp.name).unlink(missing_ok=True)
        logger.info("Archived %s -> %s", url, file_url)
        return file_url
