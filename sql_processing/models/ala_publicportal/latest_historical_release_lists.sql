MODEL (
    name ala_publicportal.latest_historical_release_lists,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, date_filed, pdf_url)
    ),
    grain (court_id, date_filed, pdf_url),
    columns (
        court_id TEXT,
        date_filed DATE,
        case_name TEXT,
        pdf_url TEXT,
        local_path TEXT,
        source_url TEXT,
        acis_doc_no TEXT,
        acis_event TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "ala_publicportal.raw_historical_release_lists",
    "ala_publicportal.raw_historical_release_lists_observations",
    "ala_publicportal.latest_historical_release_lists",
    ["court_id", "date_filed", "pdf_url"]
) }}
JINJA_END;
