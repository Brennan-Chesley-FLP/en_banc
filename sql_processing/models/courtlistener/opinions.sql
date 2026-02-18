MODEL (
    name courtlistener.opinions,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, cluster_date_filed, type, author_str)
    ),
    grain (court_id, docket_number, cluster_date_filed, type, author_str)
);

-- Alabama opinions
SELECT
    o.court_id,
    o.case_number AS docket_number,
    o.date_filed AS cluster_date_filed,
    CASE o.opinion_type
        WHEN 'majority' THEN '020lead'
        WHEN 'dissent' THEN '040dissent'
        WHEN 'concurrence' THEN '030concurrence'
        ELSE '010combined'
    END AS type,
    COALESCE(o.authoring_judge, '') AS author_str,
    o.per_curiam,
    NULL::TEXT AS joined_by_str,
    NULL::TEXT AS sha1,
    NULL::INTEGER AS page_count,
    o.opinion_download_url AS download_url,
    o.opinion_local_path AS local_path,
    NULL::TEXT AS plain_text,
    o.decision_text AS html,
    FALSE AS extracted_by_ocr,
    o.provenance_id,
    o.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM ala_publicportal.stg_opinions AS o

UNION ALL

-- Connecticut opinions
SELECT
    o.court_id,
    o.docket_id AS docket_number,
    o.date_filed AS cluster_date_filed,
    CASE o.opinion_type
        WHEN 'majority' THEN '020lead'
        WHEN 'dissent' THEN '040dissent'
        WHEN 'concurrence' THEN '030concurrence'
        ELSE '010combined'
    END AS type,
    '' AS author_str,
    FALSE AS per_curiam,
    NULL::TEXT AS joined_by_str,
    NULL::TEXT AS sha1,
    NULL::INTEGER AS page_count,
    o.opinion_download_url AS download_url,
    o.opinion_local_path AS local_path,
    NULL::TEXT AS plain_text,
    NULL::TEXT AS html,
    FALSE AS extracted_by_ocr,
    o.provenance_id,
    o.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM conn_jud_ct_gov.stg_opinions AS o;
