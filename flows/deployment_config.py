"""Per-scraper deployment customization via TOML files.

Each JKent scraper gets a Prefect deployment. By default the Pulumi program
builds that deployment from a computed *skeleton* — name, work-pool routing,
court/transport tags, and the base ``{scraper_path, scraper_schema}``
parameters. A scraper may override and extend that skeleton by dropping a TOML
file at ``infrastructure/deployments/{scraper_schema}.toml``; when present it is
deep-merged over the skeleton (the TOML wins per key), and it may additionally
declare recurring ``[[schedules]]`` and the initial cursors for any speculative
``[key]`` references its ``seed_params`` use (``[variables.<key>]`` tables).

The TOML mirrors the subset of Prefect's deployment schema that the
``pulumi_prefect`` provider can set, plus two conveniences:

* ``[[schedules]]`` — each becomes a ``DeploymentSchedule`` resource.
* ``[variables.<key>]`` — each becomes a Prefect ``Variable`` (created once by
  Pulumi with ``ignore_changes=["value"]``; the scrape's finalize step advances
  it at runtime). Every ``[key]`` referenced from ``seed_params`` must have a
  matching table here, or :func:`build_deployment_spec` raises — this fails a
  ``pulumi up`` fast instead of letting a scheduled run blow up at seed time
  with "no value for key".

This module is pure (no Pulumi imports) so it can be unit-tested and reused; the
Pulumi program turns the returned :class:`DeploymentSpec` into resources.
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

#: Top-level keys a deployment TOML may set. Structural fields (``flow_id``,
#: ``entrypoint``, ``path``) are always Pulumi-controlled and are intentionally
#: absent — a TOML cannot repoint the deployment at different code.
_ALLOWED_TOP_LEVEL = frozenset(
    {
        "description",
        "version",
        "job_variables",
        "work_pool_name",
        "work_queue_name",
        "concurrency_limit",
        "enforce_parameter_schema",
        "paused",
        "tags",
        "parameters",
        "schedules",
        "variables",
    }
)

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
    """Normalized deployment definition: skeleton merged with any TOML overrides.

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


def config_path(schema: str) -> Path:
    """Return the TOML path for a scraper schema (may not exist)."""
    return DEPLOYMENTS_DIR / f"{schema}.toml"


def load_toml(schema: str) -> dict[str, Any]:
    """Load and validate a scraper's deployment TOML, or ``{}`` if absent.

    Raises:
        ValueError: If the file has unknown top-level keys or malformed
            schedules — surfaced at ``pulumi up`` rather than at run time.
    """
    path = config_path(schema)
    if not path.exists():
        return {}
    with path.open("rb") as fh:
        data = tomllib.load(fh)

    unknown = set(data) - _ALLOWED_TOP_LEVEL
    if unknown:
        raise ValueError(
            f"{path.name}: unknown deployment key(s) {sorted(unknown)}; "
            f"allowed: {sorted(_ALLOWED_TOP_LEVEL)}"
        )
    for i, sched in enumerate(data.get("schedules", [])):
        _validate_schedule(sched, path.name, i)
    return data


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


def build_deployment_spec(
    *,
    scraper_path: str,
    schema: str,
    needs_browser: bool,
    court_ids: list[str],
    default_concurrency: int,
    browser_pool: str,
    http_pool: str,
) -> DeploymentSpec:
    """Build the deployment spec for a scraper: skeleton merged with TOML.

    The skeleton reproduces what the Pulumi program built before TOML support:
    the deployment is named after the schema, routed to the browser or HTTP work
    pool by ``needs_browser``, tagged with its transport and CourtListener
    courts, and seeded with the base ``{scraper_path, scraper_schema}``
    parameters. Any ``{schema}.toml`` deep-merges over that: scalar fields the
    TOML sets win, ``parameters`` merge key-wise (``scraper_path`` /
    ``scraper_schema`` are always re-forced so a TOML can't repoint the run),
    and ``tags`` are unioned. ``schedules`` and ``variables`` come straight from
    the TOML.

    Raises:
        ValueError: If the TOML is malformed, or a ``[key]`` referenced by the
            merged ``parameters`` has no matching ``[variables.<key>]`` table.
    """
    toml = load_toml(schema)

    work_pool = toml.get("work_pool_name") or (
        browser_pool if needs_browser else http_pool
    )
    # The queue lives under the (resolved) pool and is named after the schema;
    # a TOML may point the deployment at a differently-named queue in that pool.
    work_queue = toml.get("work_queue_name") or schema

    transport_tag = "browser" if needs_browser else "http"
    tags = _union(
        ["en-banc", "scraper", transport_tag, *(f"court:{c}" for c in court_ids)],
        toml.get("tags", []),
    )

    # Base params are always present; TOML params extend/override them, but the
    # two identity params are re-forced so a deployment can't run a different
    # scraper than the one it's named for.
    parameters = {"scraper_path": scraper_path, "scraper_schema": schema}
    parameters.update(toml.get("parameters", {}))
    parameters["scraper_path"] = scraper_path
    parameters["scraper_schema"] = schema

    variables = toml.get("variables", {})
    _check_key_refs_declared(parameters, variables, schema)

    return DeploymentSpec(
        name=schema,
        work_pool_name=work_pool,
        work_queue_name=work_queue,
        concurrency_limit=int(toml.get("concurrency_limit", default_concurrency)),
        tags=tags,
        parameters=parameters,
        enforce_parameter_schema=bool(toml.get("enforce_parameter_schema", True)),
        paused=bool(toml.get("paused", False)),
        description=toml.get("description"),
        version=toml.get("version"),
        job_variables=toml.get("job_variables"),
        schedules=list(toml.get("schedules", [])),
        variables=variables,
    )


def _check_key_refs_declared(
    parameters: dict[str, Any],
    variables: dict[str, dict[str, Any]],
    schema: str,
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
            f"{schema}.toml: seed_params reference variable key(s) "
            f"{sorted(missing)} with no matching [variables.<key>] table; "
            f"declare an initial cursor for each so Pulumi can create the "
            f"Prefect Variable."
        )


def _union(base: list[str], extra: list[str]) -> list[str]:
    """Return ``base`` followed by any items of ``extra`` not already present."""
    seen = set(base)
    return base + [x for x in extra if not (x in seen or seen.add(x))]
