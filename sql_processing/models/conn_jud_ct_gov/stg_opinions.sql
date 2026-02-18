MODEL (
    name conn_jud_ct_gov.stg_opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, opinion_download_url)
    ),
    grain (court_id, docket_id, opinion_download_url)
);

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
    r.loaded_at
FROM conn_jud_ct_gov.raw_opinion_clusters AS r
CROSS JOIN LATERAL jsonb_array_elements(r.opinions::JSONB) AS o(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
