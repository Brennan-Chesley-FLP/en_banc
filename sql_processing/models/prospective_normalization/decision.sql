MODEL (
    name prospective_normalization.decision,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        decided_date TIMESTAMPTZ,
        disposition TEXT
    )
);

SELECT
    id,
    decided_date,
    disposition
FROM prospective_normalization.decision
WHERE 1 = 0;
