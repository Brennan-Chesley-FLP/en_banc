MODEL (
    name courtlistener.irr_invalid_court_ids_dockets,
    kind FULL,
    grain (court_id, docket_number),
    depends_on (warehouse.court_ids),
    columns (
        court_id TEXT,
        docket_number TEXT,
        case_name TEXT,
        checked_at DATE
    )
);

SELECT
    d.court_id,
    d.docket_number,
    d.case_name,
    CURRENT_DATE AS checked_at
FROM courtlistener.staged_dockets AS d
WHERE d.court_id NOT IN (
    SELECT court_id FROM warehouse.court_ids
);
