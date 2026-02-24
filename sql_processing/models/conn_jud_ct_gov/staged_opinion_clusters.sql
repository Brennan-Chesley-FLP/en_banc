MODEL (
    name conn_jud_ct_gov.staged_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, date_filed)
    ),
    grain (court_id, docket_id, date_filed),
    depends_on (warehouse.court_ids),
    audits (
        assert_valid_court_ids,
        assert_dates_not_future
    ),
    columns (
        court_id TEXT,
        docket_id TEXT,
        date_filed DATE,
        case_name TEXT,
        publication_year INT,
        publication_name TEXT,
        law_journal_date TEXT,
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
    FROM conn_jud_ct_gov.staged_opinion_clusters
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM conn_jud_ct_gov.staged_opinion_clusters
),
prov_changed AS (
    SELECT court_id, docket_id, date_filed
    FROM conn_jud_ct_gov.latest_opinion_clusters, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_id, date_filed
    FROM conn_jud_ct_gov.corrections_opinion_clusters, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_id, date_filed FROM prov_changed
    UNION
    SELECT court_id, docket_id, date_filed FROM corr_changed
)
SELECT
    l.court_id,
    l.docket_id,
    @correct(c, date_filed, l.date_filed, DATE) AS date_filed,
    @correct(c, case_name, TRIM(l.case_name)) AS case_name,
    l.publication_year,
    l.publication_name,
    l.law_journal_date,
    l.source_url,
    l.opinions,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id,
    COALESCE(c.correction_id, 0) AS correction_id
FROM changed_keys AS ck
JOIN conn_jud_ct_gov.latest_opinion_clusters AS l
    ON l.court_id = ck.court_id AND l.docket_id = ck.docket_id AND l.date_filed = ck.date_filed
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_id, date_filed)
        court_id, docket_id, date_filed, correction_id, corrections
    FROM conn_jud_ct_gov.corrections_opinion_clusters
    ORDER BY court_id, docket_id, date_filed, correction_id DESC
) AS c ON c.court_id = l.court_id AND c.docket_id = l.docket_id AND c.date_filed = l.date_filed;
