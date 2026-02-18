MODEL (
    name conn_jud_ct_gov.stg_oral_arguments,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_argued)
    ),
    grain (court_id, docket_number, date_argued)
);

SELECT
    r.court_id,
    r.docket_number,
    r.date_argued,
    TRIM(r.case_name) AS case_name,
    r.download_url,
    r.local_path,
    r.court_year,
    r.term,
    r.case_detail_url,
    r.audio_id,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM conn_jud_ct_gov.raw_oral_arguments AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
