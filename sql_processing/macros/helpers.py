from sqlmesh import macro


@macro()
def correct(evaluator, corrections_alias, field_name, original_expr, cast_type=None):
    """Apply a JSONB correction if present, otherwise use the original value.

    Three states:
    - Key absent from corrections JSONB → use original value
    - Key present with value → use the correction
    - Key present with JSON null → ->>'key' returns SQL NULL

    Usage::

        @correct(c, date_filed, l.date_filed, DATE) AS date_filed
        @correct(c, case_name, TRIM(l.case_name)) AS case_name
    """
    c = corrections_alias.sql()
    field = field_name.name if hasattr(field_name, "name") else str(field_name)
    orig = original_expr.sql()
    extract = f"{c}.corrections->>'{field}'"
    if cast_type is not None:
        cast_str = cast_type.sql() if hasattr(cast_type, "sql") else str(cast_type)
        extract = f"({extract})::{cast_str}"
    return f"CASE WHEN {c}.corrections ? '{field}' THEN {extract} ELSE {orig} END"


@macro()
def resolve_latest(evaluator, raw_table, obs_table, self_table, *key_columns):
    """Generate the complete query for a latest_ model (Jinja mode).

    Produces the full SQL: WITH clause (5 CTEs) + SELECT with all domain
    columns from the raw table (excluding row_id and content_hash), plus
    provenance_id, record_id, and min_provenance_id from the best CTE.

    Must be used inside a JINJA_QUERY_BEGIN / JINJA_END block, with arguments
    passed as Python strings/lists::

        JINJA_QUERY_BEGIN;
        {{ resolve_latest(
            "schema.raw_table",
            "schema.raw_table_observations",
            "schema.latest_table",
            ["key1", "key2"]
        ) }}
        JINJA_END;
    """
    # In Jinja mode, arguments are Python strings/lists.
    raw = str(raw_table)
    obs = str(obs_table)
    self_t = str(self_table)

    if isinstance(key_columns[0], list):
        keys = key_columns[0]
    else:
        keys = list(key_columns)

    # Get domain columns from the raw table (exclude row_id and content_hash)
    exclude = {"row_id", "content_hash"}
    columns = evaluator.columns_to_types(raw)
    domain_cols = [c for c in columns if c not in exclude]
    select_cols = ",\n    ".join(f"r.{c}" for c in domain_cols)

    def is_not_distinct_join(left_alias, right_alias):
        return " AND ".join(
            f"{left_alias}.{k} IS NOT DISTINCT FROM {right_alias}.{k}"
            for k in keys
        )

    distinct_on = ", ".join(f"r.{k}" for k in keys)

    return f"""WITH watermark AS (
    SELECT COALESCE(MAX(provenance_id), 0) AS max_prov
    FROM {self_t}
),
new_observations AS (
    SELECT obs.row_id, obs.provenance_id
    FROM {obs} AS obs, watermark AS w
    WHERE obs.provenance_id > w.max_prov
),
touched_keys AS (
    SELECT DISTINCT {", ".join(f"r.{k}" for k in keys)}
    FROM new_observations AS nobs
    JOIN {raw} AS r ON r.row_id = nobs.row_id
),
best_candidate AS (
    SELECT DISTINCT ON ({distinct_on})
        r.row_id, obs.provenance_id, obs.record_id
    FROM touched_keys AS tk
    JOIN {raw} AS r ON {is_not_distinct_join("r", "tk")}
    JOIN {obs} AS obs ON obs.row_id = r.row_id
    ORDER BY {distinct_on}, obs.provenance_id DESC
),
best AS (
    SELECT
        bc.row_id,
        bc.provenance_id,
        bc.record_id,
        MIN(all_obs.provenance_id) AS min_provenance_id
    FROM best_candidate AS bc
    JOIN {raw} AS bc_raw ON bc_raw.row_id = bc.row_id
    JOIN {raw} AS r_all ON {is_not_distinct_join("r_all", "bc_raw")}
    JOIN {obs} AS all_obs ON all_obs.row_id = r_all.row_id
    GROUP BY bc.row_id, bc.provenance_id, bc.record_id
)
SELECT
    {select_cols},
    best.provenance_id,
    best.record_id,
    best.min_provenance_id
FROM best
JOIN {raw} AS r ON r.row_id = best.row_id"""
