AUDIT (
    name assert_valid_court_ids
);

SELECT *
FROM @this_model AS t
WHERE t.court_id NOT IN (
    SELECT court_id FROM warehouse.court_ids
);
