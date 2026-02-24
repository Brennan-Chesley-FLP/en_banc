MODEL (
    name ala_publicportal.staged_opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, opinion_download_url)
    ),
    grain (court_id, case_number, opinion_download_url),
    columns (
        court_id TEXT,
        case_number TEXT,
        date_filed DATE,
        case_name TEXT,
        per_curiam BOOLEAN,
        on_rehearing BOOLEAN,
        lower_court TEXT,
        lower_court_number TEXT,
        opinion_download_url TEXT,
        opinion_type TEXT,
        opinion_local_path TEXT,
        authoring_judge TEXT,
        decision_text TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM ala_publicportal.staged_opinions
)
SELECT
    r.court_id,
    r.case_number,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.per_curiam,
    r.on_rehearing,
    r.lower_court,
    r.lower_court_number,
    o.value->>'download_url' AS opinion_download_url,
    o.value->>'type' AS opinion_type,
    o.value->>'local_path' AS opinion_local_path,
    o.value->>'authoring_judge' AS authoring_judge,
    o.value->>'decision_text' AS decision_text,
    r.provenance_id,
    r.record_id,
    r.min_provenance_id
FROM ala_publicportal.latest_opinion_clusters AS r, watermark AS w
CROSS JOIN LATERAL jsonb_array_elements(r.opinions::JSONB) AS o(value)
WHERE r.provenance_id > w.max_prov;
