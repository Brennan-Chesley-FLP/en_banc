MODEL (
    name courtlistener.dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number)
    ),
    grain (court_id, docket_number),
    audits (
        assert_valid_court_ids,
        assert_no_null_docket_numbers
    )
);

-- Alabama dockets
SELECT
    d.court_id,
    d.case_number AS docket_number,
    NULL::TEXT AS docket_number_core,
    d.date_filed,
    d.case_name,
    NULL::TEXT AS case_name_short,
    NULL::TEXT AS case_name_full,
    NULL::TEXT AS slug,
    d.case_classification AS cause,
    NULL::TEXT AS nature_of_suit,
    NULL::TEXT AS assigned_to_str,
    NULL::TEXT AS referred_to_str,
    NULL::TEXT AS panel_str,
    d.originating_court AS appeal_from_str,
    NULL::TEXT AS filepath_local,
    FALSE AS blocked,
    d.provenance_id,
    d.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM ala_publicportal.stg_dockets AS d

UNION ALL

-- Connecticut dockets
SELECT
    d.court_id,
    d.docket_id AS docket_number,
    NULL::TEXT AS docket_number_core,
    d.date_filed,
    d.case_name,
    NULL::TEXT AS case_name_short,
    NULL::TEXT AS case_name_full,
    NULL::TEXT AS slug,
    d.case_type AS cause,
    NULL::TEXT AS nature_of_suit,
    NULL::TEXT AS assigned_to_str,
    NULL::TEXT AS referred_to_str,
    d.panel AS panel_str,
    d.trial_court AS appeal_from_str,
    NULL::TEXT AS filepath_local,
    FALSE AS blocked,
    d.provenance_id,
    d.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM conn_jud_ct_gov.stg_dockets AS d;
