MODEL (
    name prospective_normalization.proceeding_docket,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        proceeding_id TEXT,
        docket_id TEXT,
        sequence_order INT,
        role TEXT
    )
);

SELECT
    id,
    proceeding_id,
    docket_id,
    sequence_order,
    role
FROM prospective_normalization.proceeding_docket
WHERE 1 = 0;
