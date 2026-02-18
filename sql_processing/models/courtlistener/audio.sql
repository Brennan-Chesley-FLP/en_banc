MODEL (
    name courtlistener.audio,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_argued)
    ),
    grain (court_id, docket_number, date_argued)
);

-- Alabama oral arguments
SELECT
    a.court_id,
    a.case_number AS docket_number,
    a.case_name,
    NULL::TEXT AS case_name_short,
    NULL::TEXT AS case_name_full,
    NULL::TEXT AS judges,
    'C' AS source,
    a.youtube_url AS download_url,
    NULL::TEXT AS local_path_mp3,
    NULL::TEXT AS local_path_original_file,
    NULL::INTEGER AS duration,
    a.date_argued,
    0::SMALLINT AS stt_status,
    FALSE AS blocked,
    a.provenance_id,
    a.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM ala_publicportal.stg_oral_arguments AS a

UNION ALL

-- Connecticut oral arguments
SELECT
    a.court_id,
    a.docket_number AS docket_number,
    a.case_name,
    NULL::TEXT AS case_name_short,
    NULL::TEXT AS case_name_full,
    NULL::TEXT AS judges,
    'C' AS source,
    a.download_url,
    NULL::TEXT AS local_path_mp3,
    a.local_path AS local_path_original_file,
    NULL::INTEGER AS duration,
    a.date_argued,
    0::SMALLINT AS stt_status,
    FALSE AS blocked,
    a.provenance_id,
    a.record_id,
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM conn_jud_ct_gov.stg_oral_arguments AS a;
