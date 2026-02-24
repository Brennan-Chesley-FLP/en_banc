MODEL (
    name ala_publicportal.irr_future_dates_opinion_clusters,
    kind FULL,
    grain (court_id, case_number, date_filed),
    columns (
        court_id TEXT,
        case_number TEXT,
        date_filed DATE,
        case_name TEXT,
        checked_at DATE
    )
);

SELECT
    court_id,
    case_number,
    date_filed,
    case_name,
    CURRENT_DATE AS checked_at
FROM ala_publicportal.staged_opinion_clusters
WHERE date_filed > CURRENT_DATE;
