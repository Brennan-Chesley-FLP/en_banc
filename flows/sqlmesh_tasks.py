"""SQLMesh transforms as Prefect tasks.

Runs SQLMesh plan (schema changes + backfill) in a single task, then
emits a ``sync.prepared`` event for downstream CL sync automation.

See docs/sqlmesh_prefect_integration.md for design details.
"""

from __future__ import annotations

from prefect import flow, task, get_run_logger
from prefect.events import emit_event
from sqlmesh import Context


@task(task_run_name="plan-and-apply")
def plan_and_apply(
    project_path: str,
    environment: str = "prod",
    scraper_schema: str | None = None,
) -> list[str]:
    """Apply pending schema changes and evaluate all relevant models.

    Returns the list of model names that were evaluated.
    """
    logger = get_run_logger()

    ctx = Context(paths=project_path)
    dag = ctx.dag

    # Filter to relevant models if scraper_schema specified.
    # Model names are catalog-qualified: "analytics"."schema"."table",
    # so we check whether the schema portion appears in the name.
    if scraper_schema:
        schema_needle = f'"{scraper_schema}".'
        roots = [m for m in dag.sorted if schema_needle in m]
        relevant = set(roots)
        for root in roots:
            relevant.update(dag.downstream(root))
        select_models = [m for m in dag.sorted if m in relevant]
    else:
        select_models = None  # all models

    model_count = len(select_models) if select_models else len(dag.sorted)
    logger.info(
        "Running plan for %d models (filter=%s)",
        model_count,
        scraper_schema or "all",
    )

    ctx.plan(
        environment=environment,
        no_prompts=True,
        auto_apply=True,
        select_models=select_models,
    )

    return select_models or list(dag.sorted)


@flow(name="sqlmesh-transforms")
def sqlmesh_transforms(
    project_path: str = "sql_processing",
    environment: str = "prod",
    scraper_schema: str | None = None,
) -> None:
    """Run SQLMesh transforms and emit sync event.

    Args:
        project_path: Path to the SQLMesh project directory.
        environment: SQLMesh environment to target.
        scraper_schema: If provided, only run models in this schema
            and their downstream dependents. Skips unrelated scrapers.
    """
    logger = get_run_logger()

    models = plan_and_apply(project_path, environment, scraper_schema)

    logger.info(
        "SQLMesh transforms complete: %d models (filter=%s)",
        len(models),
        scraper_schema or "all",
    )

    # Emit sync.prepared event for downstream CL sync automation
    logger.info("Emitting sync.prepared event (schema=%s)", scraper_schema)
    emit_event(
        event="sync.prepared",
        resource={
            "prefect.resource.id": f"sqlmesh.{scraper_schema or 'all'}",
        },
    )
