MODEL (
    name prospective_normalization.docket,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        court_id TEXT,
        docket_number TEXT,
        opened_date TIMESTAMPTZ,
        closed_date TIMESTAMPTZ
    )
);

SELECT
    id,
    court_id,
    docket_number,
    opened_date,
    closed_date
FROM prospective_normalization.docket
WHERE 1 = 0;
