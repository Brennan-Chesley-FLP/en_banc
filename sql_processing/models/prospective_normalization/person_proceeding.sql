MODEL (
    name prospective_normalization.person_proceeding,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        person_id TEXT,
        proceeding_id TEXT,
        role TEXT
    )
);

SELECT
    id,
    person_id,
    proceeding_id,
    role
FROM prospective_normalization.person_proceeding
WHERE 1 = 0;
