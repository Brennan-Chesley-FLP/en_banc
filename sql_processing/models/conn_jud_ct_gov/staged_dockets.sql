MODEL (
    name conn_jud_ct_gov.staged_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id)
    ),
    grain (court_id, docket_id),
    depends_on (warehouse.court_ids),
    audits (assert_valid_court_ids),
    columns (
        court_id TEXT,
        docket_id TEXT,
        crn INT,
        date_filed DATE,
        case_name TEXT,
        status TEXT,
        appeal_by TEXT,
        disposition_method TEXT,
        argued_date DATE,
        disposition_date DATE,
        submitted_on_briefs_date DATE,
        cite TEXT,
        panel TEXT,
        response_due_date DATE,
        trial_court_docket_number TEXT,
        trial_court_docket_url TEXT,
        judgment_for TEXT,
        trial_court TEXT,
        trial_judge TEXT,
        judgment_date DATE,
        case_type TEXT,
        is_efiled BOOLEAN,
        exhibits_received_by_court DATE,
        source_url TEXT,
        subscription_url TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT,
        correction_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM conn_jud_ct_gov.staged_dockets
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM conn_jud_ct_gov.staged_dockets
),
prov_changed AS (
    SELECT court_id, docket_id
    FROM conn_jud_ct_gov.latest_dockets, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_id
    FROM conn_jud_ct_gov.corrections_dockets, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_id FROM prov_changed
    UNION
    SELECT court_id, docket_id FROM corr_changed
)
SELECT
    l.court_id,
    l.docket_id,
    l.crn,
    @correct(c, date_filed, l.date_filed, DATE) AS date_filed,
    @correct(c, case_name, TRIM(l.case_name)) AS case_name,
    @correct(c, status, l.status) AS status,
    l.appeal_by,
    l.disposition_method,
    l.argued_date,
    l.disposition_date,
    l.submitted_on_briefs_date,
    l.cite,
    l.panel,
    l.response_due_date,
    l.trial_court_docket_number,
    l.trial_court_docket_url,
    l.judgment_for,
    l.trial_court,
    l.trial_judge,
    l.judgment_date,
    l.case_type,
    l.is_efiled,
    l.exhibits_received_by_court,
    l.source_url,
    l.subscription_url,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id,
    COALESCE(c.correction_id, 0) AS correction_id
FROM changed_keys AS ck
JOIN conn_jud_ct_gov.latest_dockets AS l
    ON l.court_id = ck.court_id AND l.docket_id = ck.docket_id
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_id)
        court_id, docket_id, correction_id, corrections
    FROM conn_jud_ct_gov.corrections_dockets
    ORDER BY court_id, docket_id, correction_id DESC
) AS c ON c.court_id = l.court_id AND c.docket_id = l.docket_id;
