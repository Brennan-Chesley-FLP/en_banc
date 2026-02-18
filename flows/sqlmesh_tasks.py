"""SQLMesh transforms as per-model Prefect tasks.

Reads the SQLMesh DAG and submits each model as a separate Prefect task
with correct wait_for dependencies. The Prefect UI shows the full model
graph with dependency arrows.

See docs/sqlmesh_prefect_integration.md for design details.
"""

from __future__ import annotations

from prefect import flow, task, get_run_logger
from prefect.events import emit_event
from sqlmesh import Context


@task(
    retries=1,
    retry_delay_seconds=30,
    task_run_name="{model_name}",
)
def run_model(
    project_path: str, model_name: str, environment: str = "prod"
) -> str:
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
def plan_and_apply(project_path: str, environment: str = "prod") -> None:
    """Apply any pending schema changes before evaluating models."""
    logger = get_run_logger()
    logger.info("Creating and applying SQLMesh plan")
    ctx = Context(paths=project_path)
    ctx.plan(
        environment=environment,
        no_prompts=True,
        auto_apply=True,
    )


@flow(name="sqlmesh-transforms")
def sqlmesh_transforms(
    project_path: str = "sql_processing",
    environment: str = "prod",
    scraper_schema: str | None = None,
) -> None:
    """Run SQLMesh transforms with per-model Prefect task visibility.

    Args:
        project_path: Path to the SQLMesh project directory.
        environment: SQLMesh environment to target.
        scraper_schema: If provided, only run models in this schema
            and their downstream dependents. Skips unrelated scrapers.
    """
    logger = get_run_logger()

    # 1. Apply schema changes (new models, altered columns, etc.)
    plan_and_apply(project_path, environment)

    # 2. Read the DAG to discover models and dependencies
    ctx = Context(paths=project_path)
    dag = ctx.dag
    graph = dag.graph

    # 3. Filter to relevant models if scraper_schema specified
    if scraper_schema:
        roots = [m for m in dag.sorted if m.startswith(f"{scraper_schema}.")]
        relevant = set(roots)
        for root in roots:
            relevant.update(dag.downstream(root))
    else:
        relevant = set(dag.sorted)

    logger.info(
        "Running %d/%d models (filter=%s)",
        len(relevant),
        len(dag.sorted),
        scraper_schema or "all",
    )

    # 4. Submit each model with upstream dependencies
    futures: dict[str, object] = {}
    for model_name in dag.sorted:
        if model_name not in relevant:
            continue
        upstream_deps = graph.get(model_name, set())
        wait_for = [futures[dep] for dep in upstream_deps if dep in futures]

        futures[model_name] = run_model.submit(
            project_path,
            model_name,
            environment,
            wait_for=wait_for,
        )

    # 5. Collect results — raises on first failure
    for name, future in futures.items():
        future.result()

    # 6. Emit sync.prepared event for downstream CL sync automation
    logger.info("Emitting sync.prepared event (schema=%s)", scraper_schema)
    emit_event(
        event="sync.prepared",
        resource={
            "prefect.resource.id": f"sqlmesh.{scraper_schema or 'all'}",
        },
    )
