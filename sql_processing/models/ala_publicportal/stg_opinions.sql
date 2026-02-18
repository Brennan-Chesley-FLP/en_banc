MODEL (
    name ala_publicportal.stg_opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, case_number, opinion_download_url)
    ),
    grain (court_id, case_number, opinion_download_url)
);

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
    r.loaded_at
FROM ala_publicportal.raw_opinion_clusters AS r
CROSS JOIN LATERAL jsonb_array_elements(r.opinions::JSONB) AS o(value)
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
