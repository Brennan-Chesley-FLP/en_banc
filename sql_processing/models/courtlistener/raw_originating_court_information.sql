MODEL (
    name courtlistener.raw_originating_court_information,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number)
    ),
    grain (court_id, docket_number),
    columns (
        court_id TEXT,
        docket_number TEXT,
        lower_court_docket_number TEXT,
        court_reporter TEXT,
        assigned_to_str TEXT,
        ordering_judge_str TEXT,
        date_filed DATE,
        date_disposed DATE,
        date_judgment DATE,
        date_judgment_eod DATE,
        date_filed_noa DATE,
        date_received_coa DATE,
        provenance_id BIGINT,
        record_id BIGINT,
        correction_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM courtlistener.raw_originating_court_information
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM courtlistener.raw_originating_court_information
)

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
    d.correction_id,
    d.min_provenance_id
FROM ala_publicportal.staged_dockets AS d, prov_watermark AS pw, corr_watermark AS cw
WHERE d.originating_court IS NOT NULL
    AND (d.provenance_id > pw.max_prov OR d.correction_id > cw.max_corr)

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
    d.correction_id,
    d.min_provenance_id
FROM conn_jud_ct_gov.staged_dockets AS d, prov_watermark AS pw, corr_watermark AS cw
WHERE d.trial_court IS NOT NULL
    AND (d.provenance_id > pw.max_prov OR d.correction_id > cw.max_corr);
