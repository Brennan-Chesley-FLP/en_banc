# SQLMesh + Prefect Integration

How to surface individual SQLMesh model evaluations as discrete Prefect tasks so the DAG is visible in the Prefect UI.

## Background

SQLMesh has no built-in Prefect integration. The maintainer [closed a request](https://github.com/TobikoData/sqlmesh/issues/3819) for it as "not planned" for the OSS offering. The only native scheduler integrations are the built-in scheduler (`sqlmesh run`) and Tobiko Cloud's Airflow/Dagster facades.

However, SQLMesh's Python API exposes the full dependency graph programmatically, which is all we need.

## Key API Surface

### `Context` — the entry point

```python
from sqlmesh import Context

ctx = Context(paths="sql_processing")
```

### `context.dag` — the dependency graph

```python
dag = ctx.dag

dag.sorted          # List[str] — topologically sorted model names
dag.graph           # Dict[str, Set[str]] — {model: set_of_upstream_deps}
dag.roots           # Set[str] — models with no upstream dependencies
dag.upstream(node)  # Set[str] — all transitive upstream deps
dag.downstream(node)  # List[str] — all transitive downstream dependents
```

### `context.plan()` — apply schema changes

```python
plan = ctx.plan(
    environment="prod",
    no_prompts=True,    # required for non-interactive use
    auto_apply=True,    # required for non-interactive use
)
```

This handles model additions, removals, and schema drift. Call once at the start of a flow, not per-model.

### `context.run()` — evaluate missing intervals

```python
status = ctx.run(
    environment="prod",
    select_models=["ala_publicportal.stg_dockets"],  # target a single model
)
assert not status.is_failure
```

When called with `select_models`, SQLMesh still checks upstream intervals internally but skips them if already filled. This is the correct method for materialization.

### `context.evaluate()` — preview only, no side effects

```python
df = ctx.evaluate("ala_publicportal.stg_dockets", start="2026-02-11", end="2026-02-13", execution_time="2026-02-13")
```

Returns a DataFrame without writing to the database. Useful for testing, not for pipeline runs.

## Implementation

### One task definition, many task runs

Prefect 3's `@task` decorator defines a reusable function. Each `.submit()` call creates a separate task run in the UI. The `task_run_name` parameter controls what appears in the flow graph — so a single `run_model` function produces nodes like "ala_publicportal.stg_dockets", "courtlistener.opinions", etc.

```python
from prefect import flow, task, get_run_logger
from sqlmesh import Context


@task(
    retries=1,
    retry_delay_seconds=30,
    task_run_name="{model_name}",
)
def run_model(project_path: str, model_name: str, environment: str = "prod"):
    """Evaluate missing intervals for a single SQLMesh model."""
    logger = get_run_logger()
    logger.info("Running model: %s", model_name)
    ctx = Context(paths=project_path)
    status = ctx.run(
        environment=environment,
        select_models=[model_name],
    )
    if status.is_failure:
        raise RuntimeError(f"Model {model_name} failed")
    return model_name


@task(task_run_name="plan-and-apply")
def plan_and_apply(project_path: str, environment: str = "prod"):
    """Apply any pending schema changes before evaluating models."""
    logger = get_run_logger()
    ctx = Context(paths=project_path)
    logger.info("Creating and applying SQLMesh plan")
    ctx.plan(
        environment=environment,
        no_prompts=True,
        auto_apply=True,
    )


@flow(name="sqlmesh-transforms")
def sqlmesh_transforms(
    project_path: str = "sql_processing",
    environment: str = "prod",
):
    """Run SQLMesh transforms with per-model Prefect task visibility.

    Reads the SQLMesh DAG and submits each model as a separate Prefect
    task with correct wait_for dependencies. The Prefect UI will show
    the full model graph with dependency arrows.
    """
    # 1. Apply schema changes (additions, removals, column changes)
    plan_and_apply(project_path, environment)

    # 2. Read the DAG to discover models and their dependencies
    ctx = Context(paths=project_path)
    dag = ctx.dag
    graph = dag.graph

    # 3. Submit each model as a task, wiring upstream dependencies
    futures = {}
    for model_name in dag.sorted:
        upstream_deps = graph.get(model_name, set())
        wait_for = [futures[dep] for dep in upstream_deps if dep in futures]

        futures[model_name] = run_model.submit(
            project_path,
            model_name,
            environment,
            wait_for=wait_for,
        )

    # 4. Collect results — raises on first failure
    for name, future in futures.items():
        future.result()
```

### What appears in the Prefect UI

With the current ~20 SQLMesh models, the flow graph will show:

```
plan-and-apply
  |
  ├── ala_publicportal.stg_dockets
  │     ├── ala_publicportal.stg_docket_entries
  │     ├── ala_publicportal.stg_docket_parties
  │     └── ...
  ├── ala_publicportal.stg_opinion_clusters
  │     └── ala_publicportal.stg_opinions
  ├── conn_jud_ct_gov.stg_dockets
  │     └── ...
  │
  ├── courtlistener.dockets  (waits for all stg_dockets)
  ├── courtlistener.opinions (waits for all stg_opinions)
  └── ...
```

Each node has its own status (running/completed/failed), duration, logs, and retry history.

## Gotchas

### Context lifecycle

Create a fresh `Context` inside each task. It holds database connections and internal state that are not serializable across Prefect task boundaries. The cost of creating a `Context` is low (reads config + metadata, no heavy computation).

### Redundant upstream checks

When `context.run(select_models=["X"])` is called, SQLMesh internally checks whether X's upstream models have filled intervals. If they have (because we already ran them in earlier tasks), SQLMesh skips them. This is fast — it's a metadata check, not a re-evaluation — but it does mean each task pays a small overhead for Context initialization + interval checking.

### External models are in the DAG

External models (the `raw_*` tables declared in `external_models/`) appear in `dag.sorted` but are not in the `futures` dict since there's no task to run for them. The `if dep in futures` guard handles this — external model dependencies are silently skipped, and their downstream models run immediately.

### Plan vs Run separation

`plan(auto_apply=True)` and `run()` serve different purposes:

- **plan** — detects schema changes (new models, altered columns, removed models) and applies DDL. Needed when model definitions have changed since the last run.
- **run** — evaluates missing data intervals for existing models. This is the per-model work.

Always call `plan` once at flow start, then `run` per-model. Calling `plan` per-model would be wasteful and could cause conflicts.

## Integration with the scraper run flow

The `sqlmesh_transforms` flow replaces the monolithic `run_sqlmesh_transforms` task in the scraper run flow:

```python
@flow
def scraper_run_flow(scraper_id, params):
    artifact = run_scraper(scraper_id, params)
    validate_run(artifact.s3_path)
    provenance_id = create_provenance(scraper_id, artifact)
    load_to_warehouse(artifact.s3_path, provenance_id)

    # Instead of a single opaque task:
    sqlmesh_transforms(project_path="sql_processing")

    trigger_export()
```

## Filtering to affected models

For scraper runs that only affect one scraper's data, you can filter the DAG to avoid running unrelated models:

```python
@flow(name="sqlmesh-transforms")
def sqlmesh_transforms(
    project_path: str = "sql_processing",
    environment: str = "prod",
    scraper_schema: str | None = None,
):
    plan_and_apply(project_path, environment)

    ctx = Context(paths=project_path)
    dag = ctx.dag
    graph = dag.graph

    if scraper_schema:
        # Only run models in this scraper's schema + their downstream deps
        roots = [m for m in dag.sorted if m.startswith(f"{scraper_schema}.")]
        relevant = set(roots)
        for root in roots:
            relevant.update(dag.downstream(root))
    else:
        relevant = set(dag.sorted)

    futures = {}
    for model_name in dag.sorted:
        if model_name not in relevant:
            continue
        upstream_deps = graph.get(model_name, set())
        wait_for = [futures[dep] for dep in upstream_deps if dep in futures]
        futures[model_name] = run_model.submit(
            project_path, model_name, environment, wait_for=wait_for,
        )

    for future in futures.values():
        future.result()
```

Called as `sqlmesh_transforms(scraper_schema="ala_publicportal")` after an Alabama scraper run, this would run only the Alabama staging models and the downstream `courtlistener.*` models, skipping Connecticut entirely.