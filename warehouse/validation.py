"""Pre-load validation for scraper SQLite output.

Checks the ``results`` table produced by juriscraper before loading
into the warehouse.
"""

from __future__ import annotations

import logging
import sqlite3
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    """Summary of a scraper output validation run."""

    total_rows: int = 0
    valid_rows: int = 0
    invalid_rows: int = 0
    counts_by_type: dict[str, int] = field(default_factory=dict)

    @property
    def is_valid(self) -> bool:
        return self.valid_rows > 0


def validate_scraper_output(db_path: str) -> ValidationReport:
    """Validate a scraper's SQLite output database.

    Checks:
    - The ``results`` table exists and has rows
    - Reports counts by result_type
    - Counts invalid rows (is_valid = 0)

    Args:
        db_path: Path to the SQLite database file.

    Returns:
        A ValidationReport summarizing the results.

    Raises:
        ValueError: If the results table is empty or missing.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check table exists
    cursor.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='results'"
    )
    if cursor.fetchone() is None:
        conn.close()
        raise ValueError(f"No 'results' table in {db_path}")

    report = ValidationReport()

    # Total and validity counts
    cursor.execute("SELECT COUNT(*) FROM results")
    report.total_rows = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(*) FROM results WHERE is_valid = 1")
    report.valid_rows = cursor.fetchone()[0]

    report.invalid_rows = report.total_rows - report.valid_rows

    # Counts by type
    cursor.execute(
        "SELECT result_type, COUNT(*) FROM results "
        "WHERE is_valid = 1 GROUP BY result_type"
    )
    report.counts_by_type = dict(cursor.fetchall())

    conn.close()

    if report.total_rows == 0:
        raise ValueError(f"Results table is empty in {db_path}")

    if report.invalid_rows > 0:
        logger.warning(
            "%d/%d rows failed validation in %s",
            report.invalid_rows,
            report.total_rows,
            db_path,
        )

    logger.info(
        "Validation: %d valid rows across %d types in %s",
        report.valid_rows,
        len(report.counts_by_type),
        db_path,
    )
    for result_type, count in sorted(report.counts_by_type.items()):
        logger.info("  %s: %d rows", result_type, count)

    return report
