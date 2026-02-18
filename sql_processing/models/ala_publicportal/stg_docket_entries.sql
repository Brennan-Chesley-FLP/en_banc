MODEL (
    name ala_publicportal.stg_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, document_uuid)
    ),
    grain (court_id, case_number, document_uuid)
);

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
    r.loaded_at
FROM ala_publicportal.raw_dockets AS r
CROSS JOIN LATERAL jsonb_array_elements(r.entries::JSONB) AS e(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
