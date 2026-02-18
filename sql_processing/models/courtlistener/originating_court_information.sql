MODEL (
    name courtlistener.originating_court_information,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number)
    ),
    grain (court_id, docket_number)
);

-- Alabama originating court information (only for dockets with appeal info)
SELECT
    d.court_id,
    d.case_number AS docket_number,
    d.originating_court_number AS lower_court_docket_number,
    NULL::TEXT AS court_reporter,
    NULL::TEXT AS assigned_to_str,
    NULL::TEXT AS ordering_judge_str,
    NULL::DATE AS date_filed,
    NULL::DATE AS date_disposed,
    NULL::DATE AS date_judgment,
    NULL::DATE AS date_judgment_eod,
    NULL::DATE AS date_filed_noa,
    NULL::DATE AS date_received_coa,
    d.provenance_id,
    d.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM ala_publicportal.stg_dockets AS d
WHERE d.originating_court IS NOT NULL

UNION ALL

-- Connecticut originating court information
SELECT
    d.court_id,
    d.docket_id AS docket_number,
    d.trial_court_docket_number AS lower_court_docket_number,
    NULL::TEXT AS court_reporter,
    d.trial_judge AS assigned_to_str,
    NULL::TEXT AS ordering_judge_str,
    NULL::DATE AS date_filed,
    NULL::DATE AS date_disposed,
    d.judgment_date AS date_judgment,
    NULL::DATE AS date_judgment_eod,
    NULL::DATE AS date_filed_noa,
    NULL::DATE AS date_received_coa,
    d.provenance_id,
    d.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM conn_jud_ct_gov.stg_dockets AS d
WHERE d.trial_court IS NOT NULL;
