MODEL (
    name conn_jud_ct_gov.staged_docket_parties,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, party_name, party_type)
    ),
    grain (court_id, docket_id, party_name, party_type),
    columns (
        court_id TEXT,
        docket_id TEXT,
        party_name TEXT,
        party_type TEXT,
        party_role TEXT,
        attorneys TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM conn_jud_ct_gov.staged_docket_parties
)
SELECT
    r.court_id,
    r.docket_id,
    p.value->>'name' AS party_name,
    p.value->>'type' AS party_type,
    p.value->>'role' AS party_role,
    p.value->'attorneys' AS attorneys,
    r.provenance_id,
    r.record_id,
    r.min_provenance_id
FROM conn_jud_ct_gov.latest_dockets AS r, watermark AS w
CROSS JOIN LATERAL jsonb_array_elements(r.parties::JSONB) AS p(value)
WHERE r.provenance_id > w.max_prov;
