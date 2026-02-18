MODEL (
    name conn_jud_ct_gov.stg_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id)
    ),
    grain (court_id, docket_id),
    audits (
        assert_valid_court_ids
    )
);

SELECT
    r.court_id,
    r.docket_id,
    r.crn,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.status,
    r.appeal_by,
    r.disposition_method,
    r.argued_date,
    r.disposition_date,
    r.submitted_on_briefs_date,
    r.cite,
    r.panel,
    r.response_due_date,
    r.trial_court_docket_number,
    r.trial_court_docket_url,
    r.judgment_for,
    r.trial_court,
    r.trial_judge,
    r.judgment_date,
    r.case_type,
    r.is_efiled,
    r.exhibits_received_by_court,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM conn_jud_ct_gov.raw_dockets AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
