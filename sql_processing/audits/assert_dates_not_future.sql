AUDIT (
    name assert_dates_not_future
);

SELECT *
FROM @this_model
WHERE date_filed > CURRENT_DATE;
