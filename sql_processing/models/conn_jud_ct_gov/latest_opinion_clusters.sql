MODEL (
    name conn_jud_ct_gov.latest_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, date_filed)
    ),
    grain (court_id, docket_id, date_filed),
    columns (
        court_id TEXT,
        docket_id TEXT,
        date_filed DATE,
        case_name TEXT,
        publication_year INT,
        publication_name TEXT,
        law_journal_date TEXT,
        source_url TEXT,
        opinions TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "conn_jud_ct_gov.raw_opinion_clusters",
    "conn_jud_ct_gov.raw_opinion_clusters_observations",
    "conn_jud_ct_gov.latest_opinion_clusters",
    ["court_id", "docket_id", "date_filed"]
) }}
JINJA_END;
