MODEL (
    name courtlistener.irr_future_dates_opinion_clusters,
    kind FULL,
    grain (court_id, docket_number, date_filed),
    columns (
        court_id TEXT,
        docket_number TEXT,
        date_filed DATE,
        case_name TEXT,
        checked_at DATE
    )
);

SELECT
    court_id,
    docket_number,
    date_filed,
    case_name,
    CURRENT_DATE AS checked_at
FROM courtlistener.staged_opinion_clusters
WHERE date_filed > CURRENT_DATE;
