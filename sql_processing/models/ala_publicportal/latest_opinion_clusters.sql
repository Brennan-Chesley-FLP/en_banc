MODEL (
    name ala_publicportal.latest_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, date_filed)
    ),
    grain (court_id, case_number, date_filed),
    columns (
        court_id TEXT,
        case_number TEXT,
        date_filed DATE,
        case_name TEXT,
        publication_number TEXT,
        authoring_judge TEXT,
        decision_text TEXT,
        lower_court TEXT,
        lower_court_number TEXT,
        per_curiam BOOLEAN,
        on_rehearing BOOLEAN,
        publication_uuid TEXT,
        publication_item_uuid TEXT,
        case_instance_uuid TEXT,
        source_url TEXT,
        opinions TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "ala_publicportal.raw_opinion_clusters",
    "ala_publicportal.raw_opinion_clusters_observations",
    "ala_publicportal.latest_opinion_clusters",
    ["court_id", "case_number", "date_filed"]
) }}
JINJA_END;
