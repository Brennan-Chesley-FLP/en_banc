MODEL (
    name ala_publicportal.latest_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number)
    ),
    grain (court_id, case_number),
    columns (
        court_id TEXT,
        case_number TEXT,
        case_instance_uuid TEXT,
        date_filed DATE,
        case_name TEXT,
        case_classification TEXT,
        originating_court TEXT,
        originating_court_number TEXT,
        status TEXT,
        court_guid TEXT,
        source_url TEXT,
        parties TEXT,
        entries TEXT,
        oral_arguments TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "ala_publicportal.raw_dockets",
    "ala_publicportal.raw_dockets_observations",
    "ala_publicportal.latest_dockets",
    ["court_id", "case_number"]
) }}
JINJA_END;
