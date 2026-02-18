MODEL (
    name conn_jud_ct_gov.stg_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, activity_type, date_filed, number)
    ),
    grain (court_id, docket_id, activity_type, date_filed, number)
);

SELECT
    r.court_id,
    r.docket_id,
    r.activity_type,
    r.number,
    r.date_filed,
    r.initiated_by,
    r.description,
    r.action,
    r.action_date,
    r.notice_date,
    r.document_url,
    r.document_local_path,
    r.is_paperless,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM conn_jud_ct_gov.raw_docket_entries AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
