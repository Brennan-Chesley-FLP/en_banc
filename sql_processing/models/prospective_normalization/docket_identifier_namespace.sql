MODEL (
    name prospective_normalization.docket_identifier_namespace,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        code TEXT,
        label TEXT,
        pattern TEXT,
        description TEXT
    )
);

SELECT
    id,
    code,
    label,
    pattern,
    description
FROM prospective_normalization.docket_identifier_namespace
WHERE 1 = 0;
