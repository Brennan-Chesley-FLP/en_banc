MODEL (
    name prospective_normalization.court,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        name TEXT,
        jurisdiction TEXT,
        level TEXT
    )
);

SELECT
    id,
    name,
    jurisdiction,
    level
FROM prospective_normalization.court
WHERE 1 = 0;
