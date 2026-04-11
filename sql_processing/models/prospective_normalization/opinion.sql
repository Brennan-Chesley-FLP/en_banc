MODEL (
    name prospective_normalization.opinion,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        decision_id TEXT,
        author_id TEXT,
        opinion_type TEXT,
        body TEXT
    )
);

SELECT
    id,
    decision_id,
    author_id,
    opinion_type,
    body
FROM prospective_normalization.opinion
WHERE 1 = 0;
