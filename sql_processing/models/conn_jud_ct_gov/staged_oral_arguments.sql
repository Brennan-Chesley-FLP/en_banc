MODEL (
    name conn_jud_ct_gov.staged_oral_arguments,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_argued)
    ),
    grain (court_id, docket_number, date_argued),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_argued DATE,
        case_name TEXT,
        download_url TEXT,
        local_path TEXT,
        court_year TEXT,
        term TEXT,
        case_detail_url TEXT,
        audio_id INT,
        source_url TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT,
        correction_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM conn_jud_ct_gov.staged_oral_arguments
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM conn_jud_ct_gov.staged_oral_arguments
),
prov_changed AS (
    SELECT court_id, docket_number, date_argued
    FROM conn_jud_ct_gov.latest_oral_arguments, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number, date_argued
    FROM conn_jud_ct_gov.corrections_oral_arguments, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number, date_argued FROM prov_changed
    UNION
    SELECT court_id, docket_number, date_argued FROM corr_changed
)
SELECT
    l.court_id,
    l.docket_number,
    l.date_argued,
    @correct(c, case_name, TRIM(l.case_name)) AS case_name,
    l.download_url,
    l.local_path,
    l.court_year,
    l.term,
    l.case_detail_url,
    l.audio_id,
    l.source_url,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id,
    COALESCE(c.correction_id, 0) AS correction_id
FROM changed_keys AS ck
JOIN conn_jud_ct_gov.latest_oral_arguments AS l
    ON l.court_id = ck.court_id AND l.docket_number = ck.docket_number AND l.date_argued = ck.date_argued
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number, date_argued)
        court_id, docket_number, date_argued, correction_id, corrections
    FROM conn_jud_ct_gov.corrections_oral_arguments
    ORDER BY court_id, docket_number, date_argued, correction_id DESC
) AS c ON c.court_id = l.court_id AND c.docket_number = l.docket_number AND c.date_argued = l.date_argued;
