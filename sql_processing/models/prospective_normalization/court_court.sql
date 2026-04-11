MODEL (
    name prospective_normalization.court_court,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        from_court_id TEXT,
        to_court_id TEXT,
        type TEXT
    )
);

SELECT
    id,
    from_court_id,
    to_court_id,
    type
FROM prospective_normalization.court_court
WHERE 1 = 0;
