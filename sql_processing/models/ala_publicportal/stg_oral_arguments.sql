MODEL (
    name ala_publicportal.stg_oral_arguments,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, date_argued)
    ),
    grain (court_id, case_number, date_argued)
);

SELECT
    r.court_id,
    r.case_number,
    r.date_argued,
    TRIM(r.case_name) AS case_name,
    r.youtube_url,
    r.youtube_video_id,
    r.calendar_uuid,
    r.case_instance_uuid,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM ala_publicportal.raw_oral_arguments AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
