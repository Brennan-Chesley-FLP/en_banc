MODEL (
    name ala_publicportal.irr_invalid_court_ids_dockets,
    kind FULL,
    grain (court_id, case_number),
    depends_on (warehouse.court_ids),
    columns (
        court_id TEXT,
        case_number TEXT,
        case_name TEXT,
        checked_at DATE
    )
);

SELECT
    d.court_id,
    d.case_number,
    d.case_name,
    CURRENT_DATE AS checked_at
FROM ala_publicportal.staged_dockets AS d
WHERE d.court_id NOT IN (
    SELECT court_id FROM warehouse.court_ids
);
