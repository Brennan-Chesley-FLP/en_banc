MODEL (
    name conn_jud_ct_gov.staged_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, activity_type, date_filed, number)
    ),
    grain (court_id, docket_id, activity_type, date_filed, number),
    columns (
        court_id TEXT,
        docket_id TEXT,
        activity_type TEXT,
        number TEXT,
        date_filed DATE,
        initiated_by TEXT,
        description TEXT,
        action TEXT,
        action_date DATE,
        notice_date DATE,
        document_url TEXT,
        document_local_path TEXT,
        is_paperless BOOLEAN,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT,
        correction_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM conn_jud_ct_gov.staged_docket_entries
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM conn_jud_ct_gov.staged_docket_entries
),
prov_changed AS (
    SELECT court_id, docket_id, activity_type, date_filed, number
    FROM conn_jud_ct_gov.latest_docket_entries, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_id, activity_type, date_filed, number
    FROM conn_jud_ct_gov.corrections_docket_entries, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_id, activity_type, date_filed, number FROM prov_changed
    UNION
    SELECT court_id, docket_id, activity_type, date_filed, number FROM corr_changed
)
SELECT
    l.court_id,
    l.docket_id,
    l.activity_type,
    l.number,
    l.date_filed,
    l.initiated_by,
    l.description,
    l.action,
    l.action_date,
    l.notice_date,
    l.document_url,
    l.document_local_path,
    l.is_paperless,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id,
    COALESCE(c.correction_id, 0) AS correction_id
FROM changed_keys AS ck
JOIN conn_jud_ct_gov.latest_docket_entries AS l
    ON l.court_id = ck.court_id AND l.docket_id = ck.docket_id
    AND l.activity_type = ck.activity_type
    AND ((l.date_filed IS NULL AND ck.date_filed IS NULL) OR l.date_filed = ck.date_filed)
    AND ((l.number IS NULL AND ck.number IS NULL) OR l.number = ck.number)
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_id, activity_type, date_filed, number)
        court_id, docket_id, activity_type, date_filed, number, correction_id, corrections
    FROM conn_jud_ct_gov.corrections_docket_entries
    ORDER BY court_id, docket_id, activity_type, date_filed, number, correction_id DESC
) AS c ON c.court_id = l.court_id AND c.docket_id = l.docket_id
    AND c.activity_type = l.activity_type
    AND ((c.date_filed IS NULL AND l.date_filed IS NULL) OR c.date_filed = l.date_filed)
    AND ((c.number IS NULL AND l.number IS NULL) OR c.number = l.number);
