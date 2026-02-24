MODEL (
    name conn_jud_ct_gov.staged_opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, opinion_download_url)
    ),
    grain (court_id, docket_id, opinion_download_url),
    columns (
        court_id TEXT,
        docket_id TEXT,
        date_filed DATE,
        case_name TEXT,
        opinion_download_url TEXT,
        opinion_type TEXT,
        opinion_local_path TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM conn_jud_ct_gov.staged_opinions
)
SELECT
    r.court_id,
    r.docket_id,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    o.value->>'download_url' AS opinion_download_url,
    o.value->>'type' AS opinion_type,
    o.value->>'local_path' AS opinion_local_path,
    r.provenance_id,
    r.record_id,
    r.min_provenance_id
FROM conn_jud_ct_gov.latest_opinion_clusters AS r, watermark AS w
CROSS JOIN LATERAL jsonb_array_elements(r.opinions::JSONB) AS o(value)
WHERE r.provenance_id > w.max_prov;
