MODEL (
    name ala_publicportal.stg_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, date_filed)
    ),
    grain (court_id, case_number, date_filed),
    audits (
        assert_valid_court_ids,
        assert_dates_not_future
    )
);

SELECT
    r.court_id,
    r.case_number,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.publication_number,
    r.authoring_judge,
    r.decision_text,
    r.lower_court,
    r.lower_court_number,
    r.per_curiam,
    r.on_rehearing,
    r.publication_uuid,
    r.case_instance_uuid,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM ala_publicportal.raw_opinion_clusters AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
