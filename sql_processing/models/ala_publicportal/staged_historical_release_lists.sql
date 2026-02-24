MODEL (
    name ala_publicportal.staged_historical_release_lists,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, date_filed, pdf_url)
    ),
    grain (court_id, date_filed, pdf_url),
    columns (
        court_id TEXT,
        date_filed DATE,
        case_name TEXT,
        pdf_url TEXT,
        local_path TEXT,
        acis_doc_no TEXT,
        acis_event TEXT,
        source_url TEXT,
        provenance_id BIGINT,
        record_id BIGINT,
        min_provenance_id BIGINT
    )
);

WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM ala_publicportal.staged_historical_release_lists
)
SELECT
    l.court_id,
    l.date_filed,
    TRIM(l.case_name) AS case_name,
    l.pdf_url,
    l.local_path,
    l.acis_doc_no,
    l.acis_event,
    l.source_url,
    l.provenance_id,
    l.record_id,
    l.min_provenance_id
FROM ala_publicportal.latest_historical_release_lists AS l, watermark AS w
WHERE l.provenance_id > w.max_prov;
