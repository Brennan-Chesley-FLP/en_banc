MODEL (
    name ala_publicportal.stg_historical_release_lists,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, date_filed, pdf_url)
    ),
    grain (court_id, date_filed, pdf_url)
);

SELECT
    r.court_id,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.pdf_url,
    r.local_path,
    r.acis_doc_no,
    r.acis_event,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM ala_publicportal.raw_historical_release_lists AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
