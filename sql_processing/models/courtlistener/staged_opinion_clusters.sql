MODEL (
    name courtlistener.staged_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number, date_filed)
    ),
    grain (court_id, docket_number, date_filed),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_filed DATE,
        date_filed_is_approximate BOOLEAN,
        case_name TEXT,
        case_name_short TEXT,
        case_name_full TEXT,
        slug TEXT,
        judges TEXT,
        precedential_status TEXT,
        syllabus TEXT,
        headnotes TEXT,
        summary TEXT,
        disposition TEXT,
        procedural_history TEXT,
        attorneys TEXT,
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
    FROM courtlistener.staged_opinion_clusters
),
corr_watermark AS (
    SELECT COALESCE(MAX(version_correction), 0) AS max_corr
    FROM courtlistener.staged_opinion_clusters
),
prov_changed AS (
    SELECT court_id, docket_number, date_filed
    FROM courtlistener.raw_opinion_clusters, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number, date_filed
    FROM courtlistener.corrections_opinion_clusters, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number, date_filed FROM prov_changed
    UNION
    SELECT court_id, docket_number, date_filed FROM corr_changed
)
SELECT
    r.court_id,
    r.docket_number,
    @correct(c, date_filed, r.date_filed, DATE) AS date_filed,
    r.date_filed_is_approximate,
    @correct(c, case_name, r.case_name) AS case_name,
    @correct(c, case_name_short, r.case_name_short) AS case_name_short,
    @correct(c, case_name_full, r.case_name_full) AS case_name_full,
    r.slug,
    @correct(c, judges, r.judges) AS judges,
    @correct(c, precedential_status, r.precedential_status) AS precedential_status,
    @correct(c, syllabus, r.syllabus) AS syllabus,
    @correct(c, headnotes, r.headnotes) AS headnotes,
    @correct(c, summary, r.summary) AS summary,
    @correct(c, disposition, r.disposition) AS disposition,
    @correct(c, procedural_history, r.procedural_history) AS procedural_history,
    @correct(c, attorneys, r.attorneys) AS attorneys,
    r.blocked,
    r.provenance_id AS version_provenance,
    r.min_provenance_id AS version_provenance_min,
    GREATEST(COALESCE(r.correction_id, 0), COALESCE(c.correction_id, 0)) AS version_correction,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts::TIMESTAMPTZ AS date_created,
    @execution_ts::TIMESTAMPTZ AS date_modified
FROM changed_keys AS ck
JOIN courtlistener.raw_opinion_clusters AS r
    ON r.court_id = ck.court_id AND r.docket_number = ck.docket_number AND r.date_filed = ck.date_filed
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number, date_filed)
        court_id, docket_number, date_filed, correction_id, corrections
    FROM courtlistener.corrections_opinion_clusters
    ORDER BY court_id, docket_number, date_filed, correction_id DESC
) AS c ON c.court_id = r.court_id AND c.docket_number = r.docket_number AND c.date_filed = r.date_filed;
