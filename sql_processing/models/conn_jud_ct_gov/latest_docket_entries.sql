MODEL (
    name conn_jud_ct_gov.latest_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, activity_type, date_filed, number)
    ),
    grain (court_id, docket_id, activity_type, date_filed, number),
    columns (
        court_id TEXT,
        docket_id TEXT,
        activity_type TEXT,
        number TEXT,
        date_filed DATE,
        initiated_by TEXT,
        description TEXT,
        action TEXT,
        action_date DATE,
        notice_date DATE,
        document_url TEXT,
        document_local_path TEXT,
        is_paperless BOOLEAN,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "conn_jud_ct_gov.raw_docket_entries",
    "conn_jud_ct_gov.raw_docket_entries_observations",
    "conn_jud_ct_gov.latest_docket_entries",
    ["court_id", "docket_id", "activity_type", "date_filed", "number"]
) }}
JINJA_END;
