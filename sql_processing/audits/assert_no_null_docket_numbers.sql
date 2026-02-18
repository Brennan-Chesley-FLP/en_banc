AUDIT (
    name assert_no_null_docket_numbers
);

SELECT *
FROM @this_model
WHERE docket_number IS NULL;
