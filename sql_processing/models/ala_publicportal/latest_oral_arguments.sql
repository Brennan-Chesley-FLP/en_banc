MODEL (
    name ala_publicportal.latest_oral_arguments,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, date_argued)
    ),
    grain (court_id, case_number, date_argued),
    columns (
        court_id TEXT,
        case_number TEXT,
        date_argued DATE,
        case_name TEXT,
        youtube_url TEXT,
        youtube_video_id TEXT,
        source_url TEXT,
        calendar_uuid TEXT,
        case_instance_uuid TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

JINJA_QUERY_BEGIN;
{{ resolve_latest(
    "ala_publicportal.raw_oral_arguments",
    "ala_publicportal.raw_oral_arguments_observations",
    "ala_publicportal.latest_oral_arguments",
    ["court_id", "case_number", "date_argued"]
) }}
JINJA_END;
