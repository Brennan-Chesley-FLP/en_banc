MODEL (
    name ala_publicportal.staged_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, document_uuid)
    ),
    grain (court_id, case_number, document_uuid),
    columns (
        court_id TEXT,
        case_number TEXT,
        date_filed DATE,
        document_type TEXT,
        document_subtype TEXT,
        description TEXT,
        document_uuid TEXT,
        document_url TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM ala_publicportal.staged_docket_entries
)
SELECT
    r.court_id,
    r.case_number,
    (e.value->>'date_filed')::DATE AS date_filed,
    e.value->>'document_type' AS document_type,
    e.value->>'document_subtype' AS document_subtype,
    e.value->>'description' AS description,
    e.value->>'document_uuid' AS document_uuid,
    e.value->>'document_url' AS document_url,
    r.provenance_id,
    r.record_id,
    r.min_provenance_id
FROM ala_publicportal.latest_dockets AS r, watermark AS w
CROSS JOIN LATERAL jsonb_array_elements(r.entries::JSONB) AS e(value)
WHERE r.provenance_id > w.max_prov;
