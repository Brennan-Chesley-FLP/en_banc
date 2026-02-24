MODEL (
    name conn_jud_ct_gov.latest_oral_arguments,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_argued)
    ),
    grain (court_id, docket_number, date_argued),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_argued DATE,
        case_name TEXT,
        download_url TEXT,
        local_path TEXT,
        source_url TEXT,
        court_year TEXT,
        term TEXT,
        case_detail_url TEXT,
        audio_id INT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "conn_jud_ct_gov.raw_oral_arguments",
    "conn_jud_ct_gov.raw_oral_arguments_observations",
    "conn_jud_ct_gov.latest_oral_arguments",
    ["court_id", "docket_number", "date_argued"]
) }}
JINJA_END;
