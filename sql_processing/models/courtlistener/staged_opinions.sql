MODEL (
    name courtlistener.staged_opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, cluster_date_filed, type, author_str)
    ),
    grain (court_id, docket_number, cluster_date_filed, type, author_str),
    columns (
        court_id TEXT,
        docket_number TEXT,
        cluster_date_filed DATE,
        type TEXT,
        author_str TEXT,
        per_curiam BOOLEAN,
        joined_by_str TEXT,
        sha1 TEXT,
        page_count INT,
        download_url TEXT,
        local_path TEXT,
        plain_text TEXT,
        html TEXT,
        extracted_by_ocr BOOLEAN,
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
    FROM courtlistener.staged_opinions
),
corr_watermark AS (
    SELECT COALESCE(MAX(version_correction), 0) AS max_corr
    FROM courtlistener.staged_opinions
),
prov_changed AS (
    SELECT court_id, docket_number, cluster_date_filed, type, author_str
    FROM courtlistener.raw_opinions, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number, cluster_date_filed, type, author_str
    FROM courtlistener.corrections_opinions, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number, cluster_date_filed, type, author_str FROM prov_changed
    UNION
    SELECT court_id, docket_number, cluster_date_filed, type, author_str FROM corr_changed
)
SELECT
    r.court_id,
    r.docket_number,
    r.cluster_date_filed,
    r.type,
    @correct(c, author_str, r.author_str) AS author_str,
    r.per_curiam,
    @correct(c, joined_by_str, r.joined_by_str) AS joined_by_str,
    r.sha1,
    r.page_count,
    r.download_url,
    r.local_path,
    @correct(c, plain_text, r.plain_text) AS plain_text,
    @correct(c, html, r.html) AS html,
    r.extracted_by_ocr,
    r.provenance_id AS version_provenance,
    r.min_provenance_id AS version_provenance_min,
    GREATEST(0, COALESCE(c.correction_id, 0)) AS version_correction,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts::TIMESTAMPTZ AS date_created,
    @execution_ts::TIMESTAMPTZ AS date_modified
FROM changed_keys AS ck
JOIN courtlistener.raw_opinions AS r
    ON r.court_id = ck.court_id AND r.docket_number = ck.docket_number
    AND r.cluster_date_filed = ck.cluster_date_filed AND r.type = ck.type AND r.author_str = ck.author_str
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number, cluster_date_filed, type, author_str)
        court_id, docket_number, cluster_date_filed, type, author_str, correction_id, corrections
    FROM courtlistener.corrections_opinions
    ORDER BY court_id, docket_number, cluster_date_filed, type, author_str, correction_id DESC
) AS c ON c.court_id = r.court_id AND c.docket_number = r.docket_number
    AND c.cluster_date_filed = r.cluster_date_filed AND c.type = r.type AND c.author_str = r.author_str;
