MODEL (
    name courtlistener.irr_null_docket_numbers_dockets,
    kind FULL,
    grain (court_id, docket_number),
    columns (
        court_id TEXT,
        docket_number TEXT,
        case_name TEXT,
        checked_at DATE
    )
);

SELECT
    court_id,
    docket_number,
    case_name,
    CURRENT_DATE AS checked_at
FROM courtlistener.staged_dockets
WHERE docket_number IS NULL;
