MODEL (
    name prospective_normalization.opinion_reference,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        from_opinion_id TEXT,
        to_opinion_id TEXT,
        type TEXT,
        parenthetical TEXT
    )
);

SELECT
    id,
    from_opinion_id,
    to_opinion_id,
    type,
    parenthetical
FROM prospective_normalization.opinion_reference
WHERE 1 = 0;
