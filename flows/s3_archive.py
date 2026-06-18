"""S3 streaming archive handler for JKent file downloads.

Implements JKent's :class:`AsyncStreamingArchiveHandler` protocol so the
driver streams downloaded files (PDFs, audio, etc.) straight into the
``files`` S3 bucket instead of the local filesystem.

Objects are content-addressed — the S3 key is derived from the SHA-256 of
the bytes — so identical content uploaded twice lands on the same key
(idempotent). When the driver supplies a ``deduplication_key`` we also write
a small pointer object so a later run can skip re-downloading entirely.
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import tempfile
from pathlib import Path
from typing import AsyncIterator
from urllib.parse import urlparse

from botocore.exceptions import ClientError
from jkent.data_types import ArchiveDecision
from prefect_aws.s3 import S3Bucket

logger = logging.getLogger(__name__)

# Name of the Prefect S3Bucket block backing file downloads.
FILES_S3_BLOCK_NAME = "files"

TYPE_TO_EXTENSION = {
    "pdf": ".pdf",
    "audio": ".mp3",
    "mp3": ".mp3",
    "wav": ".wav",
    "image": ".jpg",
    "jpg": ".jpg",
    "jpeg": ".jpg",
    "png": ".png",
    "gif": ".gif",
    "html": ".html",
    "json": ".json",
    "xml": ".xml",
    "text": ".txt",
    "csv": ".csv",
}


def _extension(url: str, expected_type: str | None) -> str:
    """Determine a file extension from the URL path or ``expected_type``."""
    suffix = Path(urlparse(url).path).suffix.lower()
    if suffix:
        return suffix
    if expected_type:
        return TYPE_TO_EXTENSION.get(expected_type.lower(), "")
    return ""


class S3AsyncStreamingArchiveHandler:
    """Streams JKent file downloads to an S3 bucket.

    Args:
        client: A boto3 S3 client (typically from a loaded ``S3Bucket``
            block's credentials, so it inherits the SeaweedFS endpoint).
        bucket: Target bucket name.
        prefix: Key prefix for stored files, e.g. ``"ny_nycourts/"``.
    """

    def __init__(self, client, bucket: str, prefix: str = "") -> None:
        self._client = client
        self._bucket = bucket
        self._prefix = prefix

    # -- key helpers -------------------------------------------------------

    def _content_key(self, sha256: str, ext: str) -> str:
        # Shard by the first two hex bytes to keep prefixes shallow.
        return f"{self._prefix}{sha256[:2]}/{sha256[2:4]}/{sha256}{ext}"

    def _dedup_pointer_key(self, deduplication_key: str) -> str:
        digest = hashlib.sha256(deduplication_key.encode()).hexdigest()
        return f"{self._prefix}_dedup/{digest}"

    def _s3_uri(self, key: str) -> str:
        return f"s3://{self._bucket}/{key}"

    # -- sync helpers run via asyncio.to_thread ----------------------------

    def _lookup_dedup(self, deduplication_key: str) -> str | None:
        """Return the stored file URL for a dedup key, or None."""
        key = self._dedup_pointer_key(deduplication_key)
        try:
            obj = self._client.get_object(Bucket=self._bucket, Key=key)
        except ClientError:
            return None
        return obj["Body"].read().decode("utf-8")

    def _upload(
        self,
        tmp_path: str,
        content_key: str,
        deduplication_key: str | None,
    ) -> str:
        self._client.upload_file(tmp_path, self._bucket, content_key)
        file_url = self._s3_uri(content_key)
        if deduplication_key:
            self._client.put_object(
                Bucket=self._bucket,
                Key=self._dedup_pointer_key(deduplication_key),
                Body=file_url.encode("utf-8"),
            )
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
        # Buffer chunks to a temp file while hashing, then upload. boto3's
        # upload is sync and wants a seekable source, so we stage locally.
        sha = hashlib.sha256()
        tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
            delete=False, prefix=".s3-stream-", suffix=".tmp"
        )
        try:
            with tmp as f:
                async for chunk in chunks:
                    sha.update(chunk)
                    await asyncio.to_thread(f.write, chunk)
            content_key = self._content_key(
                sha.hexdigest(), _extension(url, expected_type)
            )
            file_url = await asyncio.to_thread(
                self._upload, tmp.name, content_key, deduplication_key
            )
        finally:
            Path(tmp.name).unlink(missing_ok=True)
        logger.info("Archived %s -> %s", url, file_url)
        return file_url


async def make_s3_archive_handler(
    prefix: str,
    block_name: str = FILES_S3_BLOCK_NAME,
) -> S3AsyncStreamingArchiveHandler:
    """Load the files S3 block and build a streaming archive handler.

    Args:
        prefix: Key prefix for uploaded files (e.g. ``"{scraper_schema}/"``).
        block_name: Name of the Prefect ``S3Bucket`` block to use.
    """
    s3_bucket = await S3Bucket.aload(block_name)
    client = s3_bucket.credentials.get_client("s3")
    return S3AsyncStreamingArchiveHandler(
        client=client, bucket=s3_bucket.bucket_name, prefix=prefix
    )
