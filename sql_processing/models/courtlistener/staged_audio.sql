MODEL (
    name courtlistener.staged_audio,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_argued)
    ),
    grain (court_id, docket_number, date_argued),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_argued DATE,
        case_name TEXT,
        case_name_short TEXT,
        case_name_full TEXT,
        judges TEXT,
        source TEXT,
        download_url TEXT,
        local_path_mp3 TEXT,
        local_path_original_file TEXT,
        duration INT,
        stt_status INT,
        blocked BOOLEAN,
        version_provenance BIGINT,
        version_provenance_min BIGINT,
        version_correction BIGINT,
        courtlistener_id BIGINT,
        date_created TIMESTAMPTZ,
        date_modified TIMESTAMPTZ
    )
);

WITH prov_watermark AS (
    SELECT COALESCE(MAX(version_provenance), 0) AS max_prov
    FROM courtlistener.staged_audio
),
corr_watermark AS (
    SELECT COALESCE(MAX(version_correction), 0) AS max_corr
    FROM courtlistener.staged_audio
),
prov_changed AS (
    SELECT court_id, docket_number, date_argued
    FROM courtlistener.raw_audio, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number, date_argued
    FROM courtlistener.corrections_audio, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number, date_argued FROM prov_changed
    UNION
    SELECT court_id, docket_number, date_argued FROM corr_changed
)
SELECT
    r.court_id,
    r.docket_number,
    r.date_argued,
    @correct(c, case_name, r.case_name) AS case_name,
    @correct(c, case_name_short, r.case_name_short) AS case_name_short,
    @correct(c, case_name_full, r.case_name_full) AS case_name_full,
    @correct(c, judges, r.judges) AS judges,
    r.source,
    r.download_url,
    r.local_path_mp3,
    r.local_path_original_file,
    r.duration,
    r.stt_status,
    r.blocked,
    r.provenance_id AS version_provenance,
    r.min_provenance_id AS version_provenance_min,
    GREATEST(COALESCE(r.correction_id, 0), COALESCE(c.correction_id, 0)) AS version_correction,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts::TIMESTAMPTZ AS date_created,
    @execution_ts::TIMESTAMPTZ AS date_modified
FROM changed_keys AS ck
JOIN courtlistener.raw_audio AS r
    ON r.court_id = ck.court_id AND r.docket_number = ck.docket_number AND r.date_argued = ck.date_argued
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number, date_argued)
        court_id, docket_number, date_argued, correction_id, corrections
    FROM courtlistener.corrections_audio
    ORDER BY court_id, docket_number, date_argued, correction_id DESC
) AS c ON c.court_id = r.court_id AND c.docket_number = r.docket_number AND c.date_argued = r.date_argued;
