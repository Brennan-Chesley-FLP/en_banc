MODEL (
    name conn_jud_ct_gov.stg_docket_parties,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, party_name, party_type)
    ),
    grain (court_id, docket_id, party_name, party_type)
);

SELECT
    r.court_id,
    r.docket_id,
    p.value->>'name' AS party_name,
    p.value->>'type' AS party_type,
    p.value->>'role' AS party_role,
    p.value->'attorneys' AS attorneys,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM conn_jud_ct_gov.raw_dockets AS r
CROSS JOIN LATERAL jsonb_array_elements(r.parties::JSONB) AS p(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
