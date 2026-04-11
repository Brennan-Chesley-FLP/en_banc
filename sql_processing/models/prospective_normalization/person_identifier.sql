MODEL (
    name prospective_normalization.person_identifier,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        person_id TEXT,
        namespace_id TEXT,
        value TEXT,
        is_primary BOOLEAN,
        valid_from TIMESTAMPTZ,
        valid_to TIMESTAMPTZ
    )
);

SELECT
    id,
    person_id,
    namespace_id,
    value,
    is_primary,
    valid_from,
    valid_to
FROM prospective_normalization.person_identifier
WHERE 1 = 0;
