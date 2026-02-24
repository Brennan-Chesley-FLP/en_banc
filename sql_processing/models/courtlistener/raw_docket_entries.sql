MODEL (
    name courtlistener.raw_docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, document_uuid)
    ),
    grain (court_id, docket_number, document_uuid),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_filed DATE,
        time_filed TEXT,
        entry_number BIGINT,
        document_uuid TEXT,
        description TEXT,
        filepath_local TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM courtlistener.raw_docket_entries
)

-- Alabama docket entries
SELECT
    e.court_id,
    e.case_number AS docket_number,
    e.date_filed,
    NULL::TIME AS time_filed,
    NULL::BIGINT AS entry_number,
    e.document_uuid,
    CONCAT_WS(' - ', e.document_type, e.document_subtype, e.description) AS description,
    e.document_url AS filepath_local,
    e.provenance_id,
    e.record_id,
    e.min_provenance_id
FROM ala_publicportal.staged_docket_entries AS e, watermark AS w
WHERE e.provenance_id > w.max_prov

UNION ALL

-- Connecticut docket entries
SELECT
    e.court_id,
    e.docket_id AS docket_number,
    e.date_filed,
    NULL::TIME AS time_filed,
    NULL::BIGINT AS entry_number,
    CONCAT(e.docket_id, '::', e.activity_type, '::', COALESCE(e.number, ''), '::', COALESCE(e.date_filed::TEXT, '')) AS document_uuid,
    CONCAT_WS(' - ', e.activity_type, e.description, e.action) AS description,
    e.document_url AS filepath_local,
    e.provenance_id,
    e.record_id,
    e.min_provenance_id
FROM conn_jud_ct_gov.staged_docket_entries AS e, watermark AS w
WHERE e.provenance_id > w.max_prov;
