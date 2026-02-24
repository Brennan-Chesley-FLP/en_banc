MODEL (
    name courtlistener.raw_opinions,
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
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM courtlistener.raw_opinions
)

-- Alabama opinions
SELECT
    o.court_id,
    o.case_number AS docket_number,
    o.date_filed AS cluster_date_filed,
    CASE o.opinion_type
        WHEN 'majority' THEN '020lead'
        WHEN 'dissent' THEN '040dissent'
        WHEN 'concurrence' THEN '030concurrence'
        ELSE '010combined'
    END AS type,
    COALESCE(o.authoring_judge, '') AS author_str,
    o.per_curiam,
    NULL::TEXT AS joined_by_str,
    NULL::TEXT AS sha1,
    NULL::INTEGER AS page_count,
    o.opinion_download_url AS download_url,
    o.opinion_local_path AS local_path,
    NULL::TEXT AS plain_text,
    o.decision_text AS html,
    FALSE AS extracted_by_ocr,
    o.provenance_id,
    o.record_id,
    o.min_provenance_id
FROM ala_publicportal.staged_opinions AS o, watermark AS w
WHERE o.provenance_id > w.max_prov

UNION ALL

-- Connecticut opinions
SELECT
    o.court_id,
    o.docket_id AS docket_number,
    o.date_filed AS cluster_date_filed,
    CASE o.opinion_type
        WHEN 'majority' THEN '020lead'
        WHEN 'dissent' THEN '040dissent'
        WHEN 'concurrence' THEN '030concurrence'
        ELSE '010combined'
    END AS type,
    '' AS author_str,
    FALSE AS per_curiam,
    NULL::TEXT AS joined_by_str,
    NULL::TEXT AS sha1,
    NULL::INTEGER AS page_count,
    o.opinion_download_url AS download_url,
    o.opinion_local_path AS local_path,
    NULL::TEXT AS plain_text,
    NULL::TEXT AS html,
    FALSE AS extracted_by_ocr,
    o.provenance_id,
    o.record_id,
    o.min_provenance_id
FROM conn_jud_ct_gov.staged_opinions AS o, watermark AS w
WHERE o.provenance_id > w.max_prov;
