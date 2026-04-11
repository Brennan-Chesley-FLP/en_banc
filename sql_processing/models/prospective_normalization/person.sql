MODEL (
    name prospective_normalization.person,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        name TEXT
    )
);

SELECT
    id,
    name
FROM prospective_normalization.person
WHERE 1 = 0;
