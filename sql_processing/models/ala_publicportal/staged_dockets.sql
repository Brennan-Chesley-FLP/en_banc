MODEL (
    name ala_publicportal.staged_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number)
    ),
    grain (court_id, case_number),
    depends_on (warehouse.court_ids),
    audits (assert_valid_court_ids),
    columns (
        court_id TEXT,
        case_number TEXT,
        case_instance_uuid TEXT,
        date_filed DATE,
        case_name TEXT,
        case_classification TEXT,
        originating_court TEXT,
        originating_court_number TEXT,
        status TEXT,
        court_guid TEXT,
        source_url TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT,
        correction_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM ala_publicportal.staged_dockets
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM ala_publicportal.staged_dockets
),
prov_changed AS (
    SELECT court_id, case_number
    FROM ala_publicportal.latest_dockets, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, case_number
    FROM ala_publicportal.corrections_dockets, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, case_number FROM prov_changed
    UNION
    SELECT court_id, case_number FROM corr_changed
)
SELECT
    l.court_id,
    l.case_number,
    l.case_instance_uuid,
    @correct(c, date_filed, l.date_filed, DATE) AS date_filed,
    @correct(c, case_name, TRIM(l.case_name)) AS case_name,
    @correct(c, case_classification, l.case_classification) AS case_classification,
    @correct(c, originating_court, l.originating_court) AS originating_court,
    @correct(c, originating_court_number, l.originating_court_number) AS originating_court_number,
    @correct(c, status, l.status) AS status,
    l.court_guid,
    l.source_url,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id,
    COALESCE(c.correction_id, 0) AS correction_id
FROM changed_keys AS ck
JOIN ala_publicportal.latest_dockets AS l
    ON l.court_id = ck.court_id AND l.case_number = ck.case_number
LEFT JOIN (
    SELECT DISTINCT ON (court_id, case_number)
        court_id, case_number, correction_id, corrections
    FROM ala_publicportal.corrections_dockets
    ORDER BY court_id, case_number, correction_id DESC
) AS c ON c.court_id = l.court_id AND c.case_number = l.case_number;
