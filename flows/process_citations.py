"""Extract case citations from opinion text and store in the warehouse.

Reads opinions with text from courtlistener.opinions, runs eyecite to
find FullCaseCitation instances, and bulk-inserts them into
courtlistener.extracted_citations.

Usage::

    uv run python -m flows.process_citations
    uv run python -m flows.process_citations --limit 500
"""

from __future__ import annotations

import logging
from itertools import islice

import psycopg2
import psycopg2.extras
from prefect import flow, task, get_run_logger
from prefect_sqlalchemy import SqlAlchemyConnector

from eyecite import get_citations
from eyecite.models import FullCaseCitation

logger = logging.getLogger(__name__)

ANALYTICS_DB_BLOCK = "analytics"

# SQL to find opinions with text that haven't been processed yet.
DISCOVER_SQL = """\
SELECT o.court_id, o.docket_number, o.cluster_date_filed,
       o.type AS opinion_type, o.author_str, o.html
FROM courtlistener.opinions o
LEFT JOIN courtlistener.extracted_citations ec
  ON o.court_id = ec.court_id
  AND o.docket_number = ec.docket_number
  AND o.cluster_date_filed = ec.cluster_date_filed
  AND o.type = ec.opinion_type
  AND o.author_str = ec.author_str
WHERE o.html IS NOT NULL
  AND ec.court_id IS NULL
ORDER BY o.date_created
LIMIT %s OFFSET %s
"""

INSERT_SQL = """\
INSERT INTO courtlistener.extracted_citations
    (court_id, docket_number, cluster_date_filed, opinion_type,
     author_str, volume, reporter, page, corrected_citation,
     year, court_from_cite, plaintiff, defendant)
VALUES %s
ON CONFLICT DO NOTHING
"""


def _batched(iterable, n):
    """Yield successive n-sized chunks from iterable."""
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            break
        yield batch


def _extract_citations(text: str) -> list[dict]:
    """Run eyecite on text and return dicts for FullCaseCitation instances."""
    citations = get_citations(text)
    results = []
    for cite in citations:
        if not isinstance(cite, FullCaseCitation):
            continue
        groups = cite.groups
        volume = groups.get("volume", "")
        reporter = groups.get("reporter", "")
        page = groups.get("page", "")
        if not (volume and reporter and page):
            continue
        results.append({
            "volume": volume,
            "reporter": reporter,
            "page": page,
            "corrected_citation": cite.corrected_citation(),
            "year": int(cite.metadata.year) if cite.metadata.year else None,
            "court_from_cite": cite.metadata.court or None,
            "plaintiff": cite.metadata.plaintiff or None,
            "defendant": cite.metadata.defendant or None,
        })
    return results


@task(log_prints=True, task_run_name="discover-opinions")
def discover_opinions(limit: int = 1000) -> list[dict]:
    """Find opinions with text that haven't had citations extracted."""
    log = get_run_logger()
    rows = []
    offset = 0
    chunk = 500

    block = SqlAlchemyConnector.load(ANALYTICS_DB_BLOCK)
    conn = psycopg2.connect(block.connection_info.create_url().render_as_string(hide_password=False))
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            while len(rows) < limit:
                cur.execute(DISCOVER_SQL, [min(chunk, limit - len(rows)), offset])
                batch = cur.fetchall()
                if not batch:
                    break
                rows.extend(batch)
                offset += len(batch)
    finally:
        conn.close()

    log.info("Found %d opinions needing citation extraction", len(rows))
    return rows


@task(log_prints=True, task_run_name="extract-and-store")
def extract_and_store_citations(opinions: list[dict]) -> dict:
    """Extract citations from a batch of opinions and store them."""
    log = get_run_logger()
    total_citations = 0
    total_opinions = 0

    block = SqlAlchemyConnector.load(ANALYTICS_DB_BLOCK)
    conn = psycopg2.connect(block.connection_info.create_url().render_as_string(hide_password=False))
    try:
        with conn.cursor() as cur:
            for opinion in opinions:
                text = opinion["html"]
                cites = _extract_citations(text)
                if not cites:
                    continue

                total_opinions += 1
                values = []
                for c in cites:
                    values.append((
                        opinion["court_id"],
                        opinion["docket_number"],
                        opinion["cluster_date_filed"],
                        opinion["opinion_type"],
                        opinion["author_str"],
                        c["volume"],
                        c["reporter"],
                        c["page"],
                        c["corrected_citation"],
                        c["year"],
                        c["court_from_cite"],
                        c["plaintiff"],
                        c["defendant"],
                    ))

                psycopg2.extras.execute_values(
                    cur, INSERT_SQL, values,
                    template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                )
                total_citations += len(values)

            conn.commit()
    finally:
        conn.close()

    log.info(
        "Extracted %d citations from %d opinions",
        total_citations,
        total_opinions,
    )
    return {"citations": total_citations, "opinions_with_cites": total_opinions}


@flow(name="extract-citations", log_prints=True)
def extract_citations_flow(
    limit: int = 1000,
    chunk_size: int = 100,
) -> dict:
    """Extract case citations from warehouse opinions.

    Discovers opinions with text that haven't been processed,
    runs eyecite, and stores results in courtlistener.extracted_citations.

    Args:
        limit: Maximum number of opinions to process.
        chunk_size: Opinions per extraction batch.

    Returns:
        Summary dict with total counts.
    """
    log = get_run_logger()
    opinions = discover_opinions(limit=limit)

    if not opinions:
        log.info("No opinions need citation extraction")
        return {"citations": 0, "opinions_with_cites": 0, "opinions_checked": 0}

    total_citations = 0
    total_with_cites = 0

    for batch in _batched(opinions, chunk_size):
        result = extract_and_store_citations(batch)
        total_citations += result["citations"]
        total_with_cites += result["opinions_with_cites"]

    summary = {
        "citations": total_citations,
        "opinions_with_cites": total_with_cites,
        "opinions_checked": len(opinions),
    }
    log.info(
        "Citation extraction complete: %d citations from %d/%d opinions",
        total_citations,
        total_with_cites,
        len(opinions),
    )
    return summary


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract citations from opinions")
    parser.add_argument("--limit", type=int, default=1000, help="Max opinions to process")
    parser.add_argument("--chunk-size", type=int, default=100, help="Batch size")
    args = parser.parse_args()

    extract_citations_flow(limit=args.limit, chunk_size=args.chunk_size)
