MODEL (
    name prospective_normalization.document,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        docket_event_id TEXT,
        opinion_id TEXT,
        resource_type TEXT,
        filename TEXT,
        original_url TEXT,
        local_url TEXT
    )
);

SELECT
    id,
    docket_event_id,
    opinion_id,
    resource_type,
    filename,
    original_url,
    local_url
FROM prospective_normalization.document
WHERE 1 = 0;
