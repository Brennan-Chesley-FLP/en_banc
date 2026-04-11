MODEL (
    name prospective_normalization.person_court,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        person_id TEXT,
        court_id TEXT,
        role TEXT
    )
);

SELECT
    id,
    person_id,
    court_id,
    role
FROM prospective_normalization.person_court
WHERE 1 = 0;
