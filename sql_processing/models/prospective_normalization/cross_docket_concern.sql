MODEL (
    name prospective_normalization.cross_docket_concern,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        from_docket_id TEXT,
        to_docket_id TEXT,
        type TEXT,
        description TEXT
    )
);

SELECT
    id,
    from_docket_id,
    to_docket_id,
    type,
    description
FROM prospective_normalization.cross_docket_concern
WHERE 1 = 0;
