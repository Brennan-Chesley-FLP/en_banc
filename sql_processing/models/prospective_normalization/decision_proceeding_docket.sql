MODEL (
    name prospective_normalization.decision_proceeding_docket,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        decision_id TEXT,
        proceeding_docket_id TEXT,
        outcome TEXT
    )
);

SELECT
    id,
    decision_id,
    proceeding_docket_id,
    outcome
FROM prospective_normalization.decision_proceeding_docket
WHERE 1 = 0;
