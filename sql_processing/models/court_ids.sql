MODEL (
    name warehouse.court_ids,
    kind SEED (
        path '../seeds/court_ids.csv'
    ),
    columns (
        court_id TEXT,
        court_name TEXT,
        jurisdiction TEXT
    ),
    grain (court_id)
);
