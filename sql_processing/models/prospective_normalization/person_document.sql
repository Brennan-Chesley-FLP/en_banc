MODEL (
    name prospective_normalization.person_document,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        person_id TEXT,
        document_id TEXT,
        role TEXT
    )
);

SELECT
    id,
    person_id,
    document_id,
    role
FROM prospective_normalization.person_document
WHERE 1 = 0;
