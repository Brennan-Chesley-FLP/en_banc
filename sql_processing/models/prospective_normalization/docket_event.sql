MODEL (
    name prospective_normalization.docket_event,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        docket_id TEXT,
        event_type TEXT,
        description TEXT,
        event_date TIMESTAMPTZ
    )
);

SELECT
    id,
    docket_id,
    event_type,
    description,
    event_date
FROM prospective_normalization.docket_event
WHERE 1 = 0;
