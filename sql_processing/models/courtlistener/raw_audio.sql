MODEL (
    name courtlistener.raw_audio,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_argued)
    ),
    grain (court_id, docket_number, date_argued),
    columns (
        court_id TEXT,
        docket_number TEXT,
        case_name TEXT,
        case_name_short TEXT,
        case_name_full TEXT,
        judges TEXT,
        source TEXT,
        download_url TEXT,
        local_path_mp3 TEXT,
        local_path_original_file TEXT,
        duration INT,
        date_argued DATE,
        stt_status INT,
        blocked BOOLEAN,
        provenance_id BIGINT,
        record_id BIGINT,
        correction_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM courtlistener.raw_audio
),
corr_watermark AS (
    SELECT COALESCE(MAX(correction_id), 0) AS max_corr
    FROM courtlistener.raw_audio
)

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
    a.correction_id,
    a.min_provenance_id
FROM ala_publicportal.staged_oral_arguments AS a, prov_watermark AS pw, corr_watermark AS cw
WHERE a.provenance_id > pw.max_prov OR a.correction_id > cw.max_corr

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
    a.correction_id,
    a.min_provenance_id
FROM conn_jud_ct_gov.staged_oral_arguments AS a, prov_watermark AS pw, corr_watermark AS cw
WHERE a.provenance_id > pw.max_prov OR a.correction_id > cw.max_corr;
