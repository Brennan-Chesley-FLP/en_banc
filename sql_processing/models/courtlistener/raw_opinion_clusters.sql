MODEL (
    name courtlistener.raw_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_filed)
    ),
    grain (court_id, docket_number, date_filed),
    depends_on (warehouse.court_ids),
    audits (
        assert_valid_court_ids,
        assert_no_null_docket_numbers,
        assert_dates_not_future
    ),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_filed DATE,
        date_filed_is_approximate BOOLEAN,
        case_name TEXT,
        case_name_short TEXT,
        case_name_full TEXT,
        slug TEXT,
        judges TEXT,
        precedential_status TEXT,
        syllabus TEXT,
        headnotes TEXT,
        summary TEXT,
        disposition TEXT,
        procedural_history TEXT,
        attorneys TEXT,
        blocked BOOLEAN,
        provenance_id BIGINT,
        record_id BIGINT,
        correction_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM courtlistener.raw_opinion_clusters
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM courtlistener.raw_opinion_clusters
)

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
    c.correction_id,
    c.min_provenance_id
FROM ala_publicportal.staged_opinion_clusters AS c, prov_watermark AS pw, corr_watermark AS cw
WHERE c.provenance_id > pw.max_prov OR c.correction_id > cw.max_corr

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
    c.correction_id,
    c.min_provenance_id
FROM conn_jud_ct_gov.staged_opinion_clusters AS c, prov_watermark AS pw, corr_watermark AS cw
WHERE c.provenance_id > pw.max_prov OR c.correction_id > cw.max_corr;
