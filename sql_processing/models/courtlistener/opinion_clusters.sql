MODEL (
    name courtlistener.opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_filed)
    ),
    grain (court_id, docket_number, date_filed),
    audits (
        assert_valid_court_ids,
        assert_no_null_docket_numbers,
        assert_dates_not_future
    )
);

-- Alabama opinion clusters
SELECT
    c.court_id,
    c.case_number AS docket_number,
    c.date_filed,
    FALSE AS date_filed_is_approximate,
    c.case_name,
    NULL::TEXT AS case_name_short,
    NULL::TEXT AS case_name_full,
    NULL::TEXT AS slug,
    c.authoring_judge AS judges,
    CASE
        WHEN c.per_curiam THEN 'Unpublished'
        ELSE 'Published'
    END AS precedential_status,
    NULL::TEXT AS syllabus,
    NULL::TEXT AS headnotes,
    NULL::TEXT AS summary,
    c.decision_text AS disposition,
    NULL::TEXT AS procedural_history,
    NULL::TEXT AS attorneys,
    FALSE AS blocked,
    c.provenance_id,
    c.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM ala_publicportal.stg_opinion_clusters AS c

UNION ALL

-- Connecticut opinion clusters
SELECT
    c.court_id,
    c.docket_id AS docket_number,
    c.date_filed,
    FALSE AS date_filed_is_approximate,
    c.case_name,
    NULL::TEXT AS case_name_short,
    NULL::TEXT AS case_name_full,
    NULL::TEXT AS slug,
    NULL::TEXT AS judges,
    'Published' AS precedential_status,
    NULL::TEXT AS syllabus,
    NULL::TEXT AS headnotes,
    NULL::TEXT AS summary,
    NULL::TEXT AS disposition,
    NULL::TEXT AS procedural_history,
    NULL::TEXT AS attorneys,
    FALSE AS blocked,
    c.provenance_id,
    c.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM conn_jud_ct_gov.stg_opinion_clusters AS c;
