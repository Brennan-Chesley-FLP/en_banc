MODEL (
    name ala_publicportal.staged_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, date_filed)
    ),
    grain (court_id, case_number, date_filed),
    depends_on (warehouse.court_ids),
    audits (
        assert_valid_court_ids,
        assert_dates_not_future
    ),
    columns (
        court_id TEXT,
        case_number TEXT,
        date_filed DATE,
        case_name TEXT,
        publication_number TEXT,
        authoring_judge TEXT,
        decision_text TEXT,
        lower_court TEXT,
        lower_court_number TEXT,
        per_curiam BOOLEAN,
        on_rehearing BOOLEAN,
        publication_uuid TEXT,
        case_instance_uuid TEXT,
        source_url TEXT,
        opinions TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT,
        correction_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM ala_publicportal.staged_opinion_clusters
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM ala_publicportal.staged_opinion_clusters
),
prov_changed AS (
    SELECT court_id, case_number, date_filed
    FROM ala_publicportal.latest_opinion_clusters, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, case_number, date_filed
    FROM ala_publicportal.corrections_opinion_clusters, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, case_number, date_filed FROM prov_changed
    UNION
    SELECT court_id, case_number, date_filed FROM corr_changed
)
SELECT
    l.court_id,
    l.case_number,
    @correct(c, date_filed, l.date_filed, DATE) AS date_filed,
    @correct(c, case_name, TRIM(l.case_name)) AS case_name,
    @correct(c, publication_number, l.publication_number) AS publication_number,
    @correct(c, authoring_judge, l.authoring_judge) AS authoring_judge,
    @correct(c, decision_text, l.decision_text) AS decision_text,
    @correct(c, lower_court, l.lower_court) AS lower_court,
    @correct(c, lower_court_number, l.lower_court_number) AS lower_court_number,
    l.per_curiam,
    l.on_rehearing,
    l.publication_uuid,
    l.case_instance_uuid,
    l.source_url,
    l.opinions,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id,
    COALESCE(c.correction_id, 0) AS correction_id
FROM changed_keys AS ck
JOIN ala_publicportal.latest_opinion_clusters AS l
    ON l.court_id = ck.court_id AND l.case_number = ck.case_number AND l.date_filed = ck.date_filed
LEFT JOIN (
    SELECT DISTINCT ON (court_id, case_number, date_filed)
        court_id, case_number, date_filed, correction_id, corrections
    FROM ala_publicportal.corrections_opinion_clusters
    ORDER BY court_id, case_number, date_filed, correction_id DESC
) AS c ON c.court_id = l.court_id AND c.case_number = l.case_number AND c.date_filed = l.date_filed;
