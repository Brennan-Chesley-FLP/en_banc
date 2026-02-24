MODEL (
    name courtlistener.raw_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number)
    ),
    grain (court_id, docket_number),
    depends_on (warehouse.court_ids),
    audits (
        assert_valid_court_ids,
        assert_no_null_docket_numbers
    ),
    columns (
        court_id TEXT,
        docket_number TEXT,
        docket_number_core TEXT,
        date_filed DATE,
        case_name TEXT,
        case_name_short TEXT,
        case_name_full TEXT,
        slug TEXT,
        cause TEXT,
        nature_of_suit TEXT,
        assigned_to_str TEXT,
        referred_to_str TEXT,
        panel_str TEXT,
        appeal_from_str TEXT,
        filepath_local TEXT,
        blocked BOOLEAN,
        provenance_id BIGINT,
        record_id BIGINT,
        correction_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM courtlistener.raw_dockets
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM courtlistener.raw_dockets
)

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
    d.correction_id,
    d.min_provenance_id
FROM ala_publicportal.staged_dockets AS d, prov_watermark AS pw, corr_watermark AS cw
WHERE d.provenance_id > pw.max_prov OR d.correction_id > cw.max_corr

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
    d.correction_id,
    d.min_provenance_id
FROM conn_jud_ct_gov.staged_dockets AS d, prov_watermark AS pw, corr_watermark AS cw
WHERE d.provenance_id > pw.max_prov OR d.correction_id > cw.max_corr;
