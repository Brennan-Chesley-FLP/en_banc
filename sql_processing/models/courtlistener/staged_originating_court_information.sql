MODEL (
    name courtlistener.staged_originating_court_information,
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
        version_provenance BIGINT,
        version_provenance_min BIGINT,
        version_correction BIGINT,
        courtlistener_id BIGINT,
        date_created TIMESTAMPTZ,
        date_modified TIMESTAMPTZ
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(version_provenance), 0) AS max_prov
    FROM courtlistener.staged_originating_court_information
),
corr_watermark AS (
    SELECT COALESCE(MAX(version_correction), 0) AS max_corr
    FROM courtlistener.staged_originating_court_information
),
prov_changed AS (
    SELECT court_id, docket_number
    FROM courtlistener.raw_originating_court_information, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number
    FROM courtlistener.corrections_originating_court_information, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number FROM prov_changed
    UNION
    SELECT court_id, docket_number FROM corr_changed
)
SELECT
    r.court_id,
    r.docket_number,
    @correct(c, lower_court_docket_number, r.lower_court_docket_number) AS lower_court_docket_number,
    @correct(c, court_reporter, r.court_reporter) AS court_reporter,
    @correct(c, assigned_to_str, r.assigned_to_str) AS assigned_to_str,
    @correct(c, ordering_judge_str, r.ordering_judge_str) AS ordering_judge_str,
    @correct(c, date_filed, r.date_filed, DATE) AS date_filed,
    @correct(c, date_disposed, r.date_disposed, DATE) AS date_disposed,
    @correct(c, date_judgment, r.date_judgment, DATE) AS date_judgment,
    @correct(c, date_judgment_eod, r.date_judgment_eod, DATE) AS date_judgment_eod,
    @correct(c, date_filed_noa, r.date_filed_noa, DATE) AS date_filed_noa,
    @correct(c, date_received_coa, r.date_received_coa, DATE) AS date_received_coa,
    r.provenance_id AS version_provenance,
    r.min_provenance_id AS version_provenance_min,
    GREATEST(COALESCE(r.correction_id, 0), COALESCE(c.correction_id, 0)) AS version_correction,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts::TIMESTAMPTZ AS date_created,
    @execution_ts::TIMESTAMPTZ AS date_modified
FROM changed_keys AS ck
JOIN courtlistener.raw_originating_court_information AS r
    ON r.court_id = ck.court_id AND r.docket_number = ck.docket_number
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number)
        court_id, docket_number, correction_id, corrections
    FROM courtlistener.corrections_originating_court_information
    ORDER BY court_id, docket_number, correction_id DESC
) AS c ON c.court_id = r.court_id AND c.docket_number = r.docket_number;
