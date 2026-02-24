MODEL (
    name conn_jud_ct_gov.latest_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id)
    ),
    grain (court_id, docket_id),
    columns (
        court_id TEXT,
        docket_id TEXT,
        crn INT,
        date_filed DATE,
        case_name TEXT,
        status TEXT,
        appeal_by TEXT,
        disposition_method TEXT,
        argued_date DATE,
        disposition_date DATE,
        submitted_on_briefs_date DATE,
        cite TEXT,
        panel TEXT,
        response_due_date DATE,
        trial_court_docket_number TEXT,
        trial_court_docket_url TEXT,
        judgment_for TEXT,
        trial_court TEXT,
        trial_judge TEXT,
        judgment_date DATE,
        case_type TEXT,
        is_efiled BOOLEAN,
        exhibits_received_by_court DATE,
        source_url TEXT,
        subscription_url TEXT,
        parties TEXT,
        preliminary_papers TEXT,
        transcripts TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "conn_jud_ct_gov.raw_dockets",
    "conn_jud_ct_gov.raw_dockets_observations",
    "conn_jud_ct_gov.latest_dockets",
    ["court_id", "docket_id"]
) }}
JINJA_END;
