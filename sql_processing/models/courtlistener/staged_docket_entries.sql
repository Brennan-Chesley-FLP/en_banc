MODEL (
    name courtlistener.staged_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, document_uuid)
    ),
    grain (court_id, docket_number, document_uuid),
    columns (
        court_id TEXT,
        docket_number TEXT,
        document_uuid TEXT,
        date_filed DATE,
        time_filed TEXT,
        entry_number BIGINT,
        description TEXT,
        filepath_local TEXT,
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
    FROM courtlistener.staged_docket_entries
),
corr_watermark AS (
    SELECT COALESCE(MAX(version_correction), 0) AS max_corr
    FROM courtlistener.staged_docket_entries
),
prov_changed AS (
    SELECT court_id, docket_number, document_uuid
    FROM courtlistener.raw_docket_entries, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number, document_uuid
    FROM courtlistener.corrections_docket_entries, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number, document_uuid FROM prov_changed
    UNION
    SELECT court_id, docket_number, document_uuid FROM corr_changed
)
SELECT
    r.court_id,
    r.docket_number,
    r.document_uuid,
    @correct(c, date_filed, r.date_filed, DATE) AS date_filed,
    r.time_filed,
    r.entry_number,
    @correct(c, description, r.description) AS description,
    r.filepath_local,
    r.provenance_id AS version_provenance,
    r.min_provenance_id AS version_provenance_min,
    GREATEST(0, COALESCE(c.correction_id, 0)) AS version_correction,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts::TIMESTAMPTZ AS date_created,
    @execution_ts::TIMESTAMPTZ AS date_modified
FROM changed_keys AS ck
JOIN courtlistener.raw_docket_entries AS r
    ON r.court_id = ck.court_id AND r.docket_number = ck.docket_number AND r.document_uuid = ck.document_uuid
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number, document_uuid)
        court_id, docket_number, document_uuid, correction_id, corrections
    FROM courtlistener.corrections_docket_entries
    ORDER BY court_id, docket_number, document_uuid, correction_id DESC
) AS c ON c.court_id = r.court_id AND c.docket_number = r.docket_number AND c.document_uuid = r.document_uuid;
