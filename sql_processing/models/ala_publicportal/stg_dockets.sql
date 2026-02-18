MODEL (
    name ala_publicportal.stg_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number)
    ),
    grain (court_id, case_number),
    audits (
        assert_valid_court_ids
    )
);

SELECT
    r.court_id,
    r.case_number,
    r.case_instance_uuid,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.case_classification,
    r.originating_court,
    r.originating_court_number,
    r.status,
    r.court_guid,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM ala_publicportal.raw_dockets AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
