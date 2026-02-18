MODEL (
    name ala_publicportal.stg_docket_parties,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, party_name, party_role)
    ),
    grain (court_id, case_number, party_name, party_role)
);

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
    r.loaded_at
FROM ala_publicportal.raw_dockets AS r
CROSS JOIN LATERAL jsonb_array_elements(r.parties::JSONB) AS p(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
