"""S3 archive callback for scraper file uploads."""

from __future__ import annotations

import io
import logging
from pathlib import Path
from urllib.parse import urlparse
from uuid import uuid4

from prefect_aws.s3 import S3Bucket

logger = logging.getLogger(__name__)

S3_BLOCK_NAME = "scrapers"

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


def _get_extension(url: str, expected_type: str | None) -> str:
    """Determine file extension from URL or expected_type hint."""
    parsed_url = urlparse(url)
    url_path = Path(parsed_url.path)
    extension = url_path.suffix.lower() if url_path.suffix else ""
    if not extension and expected_type:
        extension = TYPE_TO_EXTENSION.get(expected_type.lower(), "")
    return extension


def make_s3_archive_callback(s3_bucket: S3Bucket, s3_file_prefix: str):
    """Create an on_archive callback that uploads files to S3.

    Args:
        s3_bucket: Loaded Prefect S3Bucket block.
        s3_file_prefix: S3 key prefix for uploaded files.

    Returns:
        Async callback matching kent's on_archive signature.
    """

    async def s3_archive_callback(
        content: bytes,
        url: str,
        expected_type: str | None,
        storage_dir: Path,
    ) -> str:
        file_uuid = uuid4()
        extension = _get_extension(url, expected_type)
        filename = f"{file_uuid}{extension}"
        s3_key = f"{s3_file_prefix}{filename}"

        s3_bucket.upload_from_file_object(
            io.BytesIO(content),
            to_path=s3_key,
        )

        bucket_name = s3_bucket.bucket_name
        return f"s3://{bucket_name}/{s3_key}"

    return s3_archive_callback
