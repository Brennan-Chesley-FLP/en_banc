"""Litestream subprocess management for SQLite replication to S3."""

from __future__ import annotations

import logging
import os
import signal
import subprocess
import time
from pathlib import Path

import yaml

logger = logging.getLogger(__name__)


def _get_s3_endpoint() -> str:
    """Determine the S3 endpoint based on environment.

    Defaults to localhost:4566 (host networking mode).
    Override with LITESTREAM_S3_ENDPOINT env var.
    """
    return os.environ.get("LITESTREAM_S3_ENDPOINT", "http://localhost:4566")


def _get_aws_env() -> dict[str, str]:
    """Build environment dict with AWS credentials for litestream."""
    return {
        **os.environ,
        "AWS_ACCESS_KEY_ID": os.environ.get("AWS_ACCESS_KEY_ID", "test"),
        "AWS_SECRET_ACCESS_KEY": os.environ.get(
            "AWS_SECRET_ACCESS_KEY", "test"
        ),
    }


def build_litestream_config(
    db_path: Path,
    s3_bucket: str,
    s3_path: str,
) -> dict:
    """Build litestream configuration dictionary.

    Args:
        db_path: Path to the SQLite database file.
        s3_bucket: S3 bucket name.
        s3_path: S3 key prefix for the replica.

    Returns:
        Dictionary suitable for writing as litestream YAML config.
    """
    return {
        "dbs": [
            {
                "path": str(db_path),
                "replicas": [
                    {
                        "type": "s3",
                        "bucket": s3_bucket,
                        "path": s3_path,
                        "endpoint": _get_s3_endpoint(),
                        "force-path-style": True,
                    }
                ],
            }
        ]
    }


def _write_temp_config(
    db_path: Path, s3_bucket: str, s3_path: str
) -> Path:
    """Write a temporary litestream config for one-off CLI commands.

    The litestream CLI (snapshots, restore) has no ``-endpoint`` flag,
    so a config file is the only way to point it at a custom S3 endpoint.
    """
    import tempfile

    config = build_litestream_config(db_path, s3_bucket, s3_path)
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".yml", delete=False
    )
    tmp.write(yaml.dump(config))
    tmp.close()
    return Path(tmp.name)


def check_replica_exists(s3_bucket: str, s3_path: str) -> bool:
    """Check if an S3 replica exists for restore.

    Uses litestream snapshots command with a temp config to route
    requests through the configured S3 endpoint.

    Args:
        s3_bucket: S3 bucket name.
        s3_path: S3 key prefix for the replica.

    Returns:
        True if a restorable replica exists.
    """
    # db_path doesn't matter for snapshots — just needs to be in the config
    config_file = _write_temp_config(
        Path("/tmp/dummy.db"), s3_bucket, s3_path
    )
    try:
        result = subprocess.run(
            [
                "litestream",
                "snapshots",
                "-config",
                str(config_file),
                "/tmp/dummy.db",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=_get_aws_env(),
        )
        return result.returncode == 0 and bool(result.stdout.strip())
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False
    finally:
        config_file.unlink(missing_ok=True)


def restore_from_replica(
    db_path: Path,
    s3_bucket: str,
    s3_path: str,
) -> bool:
    """Restore SQLite database from S3 replica.

    Args:
        db_path: Path to write the restored database.
        s3_bucket: S3 bucket name.
        s3_path: S3 key prefix for the replica.

    Returns:
        True if restore succeeded, False otherwise.
    """
    logger.info(
        "Restoring SQLite from S3 replica: s3://%s/%s", s3_bucket, s3_path
    )
    config_file = _write_temp_config(db_path, s3_bucket, s3_path)
    try:
        result = subprocess.run(
            [
                "litestream",
                "restore",
                "-config",
                str(config_file),
                "-o",
                str(db_path),
                str(db_path),
            ],
            capture_output=True,
            text=True,
            timeout=300,
            env=_get_aws_env(),
        )
        if result.returncode == 0:
            logger.info("Restored SQLite database to %s", db_path)
            return True
        else:
            logger.warning("Restore failed: %s", result.stderr)
            return False
    except (subprocess.TimeoutExpired, FileNotFoundError) as e:
        logger.warning("Restore failed: %s", e)
        return False
    finally:
        config_file.unlink(missing_ok=True)


class LitestreamReplicator:
    """Manages a litestream replicate subprocess."""

    def __init__(self, config_path: Path) -> None:
        self._config_path = config_path
        self._process: subprocess.Popen | None = None

    def start(self) -> None:
        """Start litestream replication as a background subprocess."""
        logger.info("Starting litestream replication")
        self._process = subprocess.Popen(
            ["litestream", "replicate", "-config", str(self._config_path)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=_get_aws_env(),
        )
        # Give it a moment to start
        time.sleep(1)
        if self._process.poll() is not None:
            stderr = (
                self._process.stderr.read().decode()
                if self._process.stderr
                else ""
            )
            raise RuntimeError(f"Litestream failed to start: {stderr}")
        logger.info(
            "Litestream replication started (PID %d)", self._process.pid
        )

    def stop(self) -> None:
        """Stop litestream gracefully via SIGINT."""
        if self._process is None:
            return
        logger.info(
            "Stopping litestream replication (PID %d)", self._process.pid
        )
        self._process.send_signal(signal.SIGINT)
        try:
            self._process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            logger.warning("Litestream did not stop gracefully, killing")
            self._process.kill()
            self._process.wait(timeout=10)
        logger.info("Litestream replication stopped")


def write_config(config: dict, config_path: Path) -> None:
    """Write a litestream config dict to a YAML file.

    Args:
        config: Litestream configuration dictionary.
        config_path: Path to write the YAML config file.
    """
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(yaml.dump(config))
