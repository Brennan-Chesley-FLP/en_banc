MODEL (
    name courtlistener.staged_dockets,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_number)
    ),
    grain (court_id, docket_number),
    columns (
        court_id TEXT,
        docket_number TEXT,
        docket_number_core TEXT,
        date_filed DATE,
        case_name TEXT,
        case_name_short TEXT,
        case_name_full TEXT,
        slug TEXT,
        cause TEXT,
        nature_of_suit TEXT,
        assigned_to_str TEXT,
        referred_to_str TEXT,
        panel_str TEXT,
        appeal_from_str TEXT,
        filepath_local TEXT,
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
    FROM courtlistener.staged_dockets
),
corr_watermark AS (
    SELECT COALESCE(MAX(version_correction), 0) AS max_corr
    FROM courtlistener.staged_dockets
),
prov_changed AS (
    SELECT court_id, docket_number
    FROM courtlistener.raw_dockets, prov_watermark
    WHERE provenance_id > prov_watermark.max_prov
),
corr_changed AS (
    SELECT court_id, docket_number
    FROM courtlistener.corrections_dockets, corr_watermark
    WHERE correction_id > corr_watermark.max_corr
),
changed_keys AS (
    SELECT court_id, docket_number FROM prov_changed
    UNION
    SELECT court_id, docket_number FROM corr_changed
)
SELECT
    r.court_id,
    r.docket_number,
    @correct(c, docket_number_core, r.docket_number_core) AS docket_number_core,
    @correct(c, date_filed, r.date_filed, DATE) AS date_filed,
    @correct(c, case_name, r.case_name) AS case_name,
    @correct(c, case_name_short, r.case_name_short) AS case_name_short,
    @correct(c, case_name_full, r.case_name_full) AS case_name_full,
    r.slug,
    @correct(c, cause, r.cause) AS cause,
    @correct(c, nature_of_suit, r.nature_of_suit) AS nature_of_suit,
    @correct(c, assigned_to_str, r.assigned_to_str) AS assigned_to_str,
    @correct(c, referred_to_str, r.referred_to_str) AS referred_to_str,
    @correct(c, panel_str, r.panel_str) AS panel_str,
    @correct(c, appeal_from_str, r.appeal_from_str) AS appeal_from_str,
    r.filepath_local,
    r.blocked,
    r.provenance_id AS version_provenance,
    r.min_provenance_id AS version_provenance_min,
    GREATEST(COALESCE(r.correction_id, 0), COALESCE(c.correction_id, 0)) AS version_correction,
    NULL::BIGINT AS courtlistener_id,
    @execution_ts::TIMESTAMPTZ AS date_created,
    @execution_ts::TIMESTAMPTZ AS date_modified
FROM changed_keys AS ck
JOIN courtlistener.raw_dockets AS r
    ON r.court_id = ck.court_id AND r.docket_number = ck.docket_number
LEFT JOIN (
    SELECT DISTINCT ON (court_id, docket_number)
        court_id, docket_number, correction_id, corrections
    FROM courtlistener.corrections_dockets
    ORDER BY court_id, docket_number, correction_id DESC
) AS c ON c.court_id = r.court_id AND c.docket_number = r.docket_number;
