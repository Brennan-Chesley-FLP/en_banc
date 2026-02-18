MODEL (
    name courtlistener.docket_entries,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, document_uuid)
    ),
    grain (court_id, docket_number, document_uuid)
);

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
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM ala_publicportal.stg_docket_entries AS e

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
    1 AS warehouse_version,
    0 AS courtlistener_version,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts AS date_created,
    @execution_ts AS date_modified
FROM conn_jud_ct_gov.stg_docket_entries AS e;
