MODEL (
    name ala_publicportal.staged_docket_parties,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, party_name, party_role)
    ),
    grain (court_id, case_number, party_name, party_role),
    columns (
        court_id TEXT,
        case_number TEXT,
        party_name TEXT,
        party_type TEXT,
        party_role TEXT,
        party_status TEXT,
        pro_se BOOLEAN,
        attorneys TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM ala_publicportal.staged_docket_parties
)
SELECT
    r.court_id,
    r.case_number,
    p.value->>'name' AS party_name,
    p.value->>'type' AS party_type,
    p.value->>'role' AS party_role,
    p.value->>'status' AS party_status,
    (p.value->>'pro_se')::BOOLEAN AS pro_se,
    p.value->'attorneys' AS attorneys,
    r.provenance_id,
    r.record_id,
    r.min_provenance_id
FROM ala_publicportal.latest_dockets AS r, watermark AS w
CROSS JOIN LATERAL jsonb_array_elements(r.parties::JSONB) AS p(value)
WHERE r.provenance_id > w.max_prov;
