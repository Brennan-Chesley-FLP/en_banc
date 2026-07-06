"""Per-scraper deployment definitions loaded from explicit TOML files.

Each TOML in ``infrastructure/deployments/`` is the *complete* definition of one
Prefect deployment — nothing is inferred from the scraper class. The Pulumi
program (``infrastructure/__main__.py``) enumerates them with
:func:`deployment_paths` and turns each into resources via
:func:`load_deployment_spec`: one deployment, one concurrency-limited work
queue, plus any ``[[schedules]]`` and speculative-cursor ``[variables.<key>]``
the file declares. No file, no deployment.

The TOML mirrors the subset of Prefect's deployment schema that the
``pulumi_prefect`` provider can set, plus two conveniences:

* ``[[schedules]]`` — each becomes a ``DeploymentSchedule`` resource.
* ``[variables.<key>]`` — each becomes a Prefect ``Variable`` (created once by
  Pulumi with ``ignore_changes=["value"]``; the scrape's finalize step advances
  it at runtime). Every ``[key]`` referenced from ``seed_params`` must have a
  matching table here, or :func:`load_deployment_spec` raises — this fails a
  ``pulumi up`` fast instead of letting a scheduled run blow up at seed time
  with "no value for key".

The filename (minus ``.toml``) is the scraper's schema slug and names the
deployment; ``parameters.scraper_schema`` must match it, so a copied file
can't silently deploy under the wrong identity.

This module is pure (no Pulumi imports) so it can be unit-tested and reused;
the Pulumi program turns each returned :class:`DeploymentSpec` into resources.
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

#: Directory holding the per-scraper ``{scraper_schema}.toml`` files, resolved
#: relative to the repo root (this module lives in ``flows/``).
DEPLOYMENTS_DIR = Path(__file__).resolve().parent.parent / "infrastructure" / "deployments"

#: Top-level keys every deployment TOML must set. Structural fields
#: (``flow_id``, ``entrypoint``, ``path``) are always Pulumi-controlled and are
#: intentionally absent — a TOML cannot repoint the deployment at different code.
_REQUIRED_TOP_LEVEL = frozenset(
    {
        "work_pool_name",
        "work_queue_name",
        "concurrency_limit",
        "tags",
        "parameters",
    }
)

#: Top-level keys a deployment TOML may set.
_ALLOWED_TOP_LEVEL = _REQUIRED_TOP_LEVEL | frozenset(
    {
        "description",
        "version",
        "job_variables",
        "enforce_parameter_schema",
        "paused",
        "schedules",
        "variables",
    }
)

#: Keys every deployment TOML must set — routing, tags, and identity are all
#: spelled out explicitly (nothing is inferred from the scraper class).
_REQUIRED_TOP_LEVEL =set([
    "work_pool_name",
    "work_queue_name",
    "concurrency_limit",
    "tags",
    "parameters",
])

#: Keys a single ``[[schedules]]`` entry may set — the subset of
#: ``DeploymentSchedule`` inputs that make sense to declare statically.
_ALLOWED_SCHEDULE_KEYS = frozenset(
    {
        "cron",
        "interval",
        "rrule",
        "timezone",
        "active",
        "slug",
        "anchor_date",
        "day_or",
        "catchup",
        "max_active_runs",
        "max_scheduled_runs",
        "parameters",
    }
)

#: Exactly one of these must be present on each schedule.
_SCHEDULE_KINDS = ("cron", "interval", "rrule")

#: A whole-string ``[key]`` reference, matching the resolver in
#: ``juriscraper.state.common.params``: a single key in square brackets, the
#: key itself free of brackets. Surrounding whitespace is tolerated.
_KEY_REF_RE = re.compile(r"\s*\[([^\[\]]+)\]\s*")


@dataclass
class DeploymentSpec:
    """One deployment definition, loaded and validated from its TOML file.

    ``parameters``/``job_variables`` are plain dicts here; the Pulumi program
    JSON-encodes them where the provider wants a string. ``schedules`` is a list
    of validated pass-through dicts (one per ``DeploymentSchedule``), and
    ``variables`` maps each Prefect Variable name to its initial value dict.
    """

    name: str
    work_pool_name: str
    work_queue_name: str
    concurrency_limit: int
    tags: list[str]
    parameters: dict[str, Any]
    enforce_parameter_schema: bool = True
    paused: bool = False
    description: str | None = None
    version: str | None = None
    job_variables: dict[str, Any] | None = None
    schedules: list[dict[str, Any]] = field(default_factory=list)
    variables: dict[str, dict[str, Any]] = field(default_factory=dict)



def deployment_paths(directory: Path = DEPLOYMENTS_DIR) -> list[Path]:
    """Return the deployment TOML paths in *directory*, sorted by name."""
    return sorted(directory.glob("*.toml"))


def load_deployment_spec(path: Path) -> DeploymentSpec:
    """Load and validate one deployment TOML into a :class:`DeploymentSpec`.

    The filename stem is the scraper schema: it becomes the deployment name and
    must equal ``parameters.scraper_schema``.

    Raises:
        ValueError: If the file has unknown or missing keys, malformed
            schedules, identity parameters that disagree with the filename, or
            a ``[key]`` reference with no matching ``[variables.<key>]`` table —
            all surfaced at ``pulumi up`` rather than at run time.
    """
    schema = path.stem
    with path.open("rb") as fh:
        data = tomllib.load(fh)

    unknown = set(data) - _ALLOWED_TOP_LEVEL
    if unknown:
        raise ValueError(
            f"{path.name}: unknown deployment key(s) {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_TOP_LEVEL)}"
        )
    missing = _REQUIRED_TOP_LEVEL - set(data)
    if missing:
        raise ValueError(
            f"{path.name}: missing required deployment key(s) {sorted(missing)}"
        )
    for i, sched in enumerate(data.get("schedules", [])):
        _validate_schedule(sched, path.name, i)

    parameters = data["parameters"]
    _check_identity_params(parameters, schema, path.name)

    variables = data.get("variables", {})
    _check_key_refs_declared(parameters, variables, path.name)

    return DeploymentSpec(
        name=schema,
        work_pool_name=data["work_pool_name"],
        work_queue_name=data["work_queue_name"],
        concurrency_limit=int(data["concurrency_limit"]),
        tags=list(data["tags"]),
        parameters=parameters,
        enforce_parameter_schema=bool(data.get("enforce_parameter_schema", True)),
        paused=bool(data.get("paused", False)),
        description=data.get("description"),
        version=data.get("version"),
        job_variables=data.get("job_variables"),
        schedules=list(data.get("schedules", [])),
        variables=variables,
    )


def _check_identity_params(
    parameters: dict[str, Any], schema: str, filename: str
) -> None:
    """Ensure the identity parameters are present and agree with the filename.

    ``scraper_schema`` names the S3 prefix, and the filename names the
    deployment and work queue — requiring them to match keeps every alias of a
    scraper's identity in agreement and catches an unedited template copy.
    """
    if not parameters.get("scraper_path"):
        raise ValueError(f"{filename}: [parameters] must set scraper_path")
    declared = parameters.get("scraper_schema")
    if declared != schema:
        raise ValueError(
            f"{filename}: parameters.scraper_schema {declared!r} must equal "
            f"the filename stem {schema!r}"
        )


def _validate_schedule(sched: Any, filename: str, index: int) -> None:
    """Validate one ``[[schedules]]`` entry: known keys, exactly one kind."""
    if not isinstance(sched, dict):
        raise ValueError(f"{filename}: schedule #{index} must be a table")
    unknown = set(sched) - _ALLOWED_SCHEDULE_KEYS
    if unknown:
        raise ValueError(
            f"{filename}: schedule #{index} has unknown key(s) {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_SCHEDULE_KEYS)}"
        )
    kinds = [k for k in _SCHEDULE_KINDS if sched.get(k) is not None]
    if len(kinds) != 1:
        raise ValueError(
            f"{filename}: schedule #{index} must set exactly one of "
            f"{_SCHEDULE_KINDS}, got {kinds or 'none'}"
        )


def _iter_key_refs(value: Any) -> set[str]:
    """Collect every ``[key]`` reference among the string leaves of ``value``."""
    refs: set[str] = set()
    if isinstance(value, str):
        m = _KEY_REF_RE.fullmatch(value)
        if m is not None:
            refs.add(m.group(1))
    elif isinstance(value, dict):
        for v in value.values():
            refs |= _iter_key_refs(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            refs |= _iter_key_refs(v)
    return refs


def _check_key_refs_declared(
    parameters: dict[str, Any],
    variables: dict[str, dict[str, Any]],
    filename: str,
) -> None:
    """Ensure every ``[key]`` in ``parameters`` has a ``[variables.<key>]`` seed.

    A ``[key]`` reference is only a pointer — the scrape resolves it against a
    Prefect Variable at seed time, so an undeclared key is a run-time failure
    waiting to happen. Declaring the seed in the same TOML lets Pulumi create
    the Variable up front, so we require it and fail the ``pulumi up`` instead.
    """
    referenced = _iter_key_refs(parameters)
    missing = referenced - set(variables)
    if missing:
        raise ValueError(
            f"{filename}: seed_params reference variable key(s) "
            f"{sorted(missing)} with no matching [variables.<key>] table; "
            f"declare an initial cursor for each so Pulumi can create the "
            f"Prefect Variable."
        )
