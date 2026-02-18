MODEL (
    name conn_jud_ct_gov.stg_opinion_clusters,
    kind INCREMENTAL_BY_UNIQUE_KEY (
        unique_key (court_id, docket_id, date_filed)
    ),
    grain (court_id, docket_id, date_filed),
    audits (
        assert_valid_court_ids,
        assert_dates_not_future
    )
);

SELECT
    r.court_id,
    r.docket_id,
    r.date_filed,
    TRIM(r.case_name) AS case_name,
    r.publication_year,
    r.publication_name,
    r.law_journal_date,
    r.source_url,
    r.provenance_id,
    r.record_id,
    r.loaded_at
FROM conn_jud_ct_gov.raw_opinion_clusters AS r
WHERE r.loaded_at >= @start_date AND r.loaded_at < @end_date;
