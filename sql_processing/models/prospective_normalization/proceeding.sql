MODEL (
    name prospective_normalization.proceeding,
    kind FULL,
    grain (id),
    columns (
        id TEXT,
        case_name TEXT,
        status TEXT,
        filed_date TIMESTAMPTZ
    )
);

SELECT
    id,
    case_name,
    status,
    filed_date
FROM prospective_normalization.proceeding
WHERE 1 = 0;
