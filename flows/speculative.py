"""Speculative-cursor persistence for scheduled maintenance scrapes.

A scraper's ``seed_params`` may reference a *persisted speculative cursor* with
a ``"[key]"`` string (see
``juriscraper.state.common.params.PersistedSpeculativeRange``). This module is
the runtime half of that mechanism:

* **Seeding** — :func:`load_seed_store` prefetches the Prefect Variables a run's
  ``seed_params`` reference into an in-memory :class:`_DictStore`, which the
  scrape's seeding step activates via ``spec_kv_store(...)`` so each ``[key]``
  resolves to its stored cursor. (Prefetching keeps the *synchronous* Pydantic
  validation off Prefect's async Variable client.)

* **Finalizing** — :func:`advance_speculative_cursors` runs after a clean
  scrape, before the DB is archived. It reads the run's stored ``seed_params``
  and speculation state, works out which Variable each speculative template came
  from (via a probing store — see below), computes the next starting point from
  the highest ID that succeeded, and writes it back to the Variable so the next
  scheduled run picks up where this one left off.

jkent persists each speculation-state row's raw seed value
(``seed_value_json`` — for a persisted cursor, the ``"[key]"`` reference
string itself), so :func:`_map_state_keys_to_vars` reads the mapping straight
off the run's speculation state; nothing is re-derived or re-validated.

All ``juriscraper`` / ``jkent`` imports are deferred into function bodies: the
``[key]`` params API ships in a juriscraper change that may land after this
module, and this file must import cleanly regardless.
"""

from __future__ import annotations

import json
import re
from datetime import date
from pathlib import Path
from typing import Any

# A whole-string ``[key]`` reference, matching the resolver in
# ``juriscraper.state.common.params`` (a single bracketed key, no nested
# brackets; surrounding whitespace tolerated).
_KEY_REF_RE = re.compile(r"\s*\[([^\[\]]+)\]\s*")


def variable_keys_in(seed_params: list[dict[str, dict[str, Any]]] | None) -> set[str]:
    """Return every ``[key]`` reference among the leaves of ``seed_params``."""
    return _collect_refs(seed_params)


def _collect_refs(value: Any) -> set[str]:
    refs: set[str] = set()
    if isinstance(value, str):
        m = _KEY_REF_RE.fullmatch(value)
        if m is not None:
            refs.add(m.group(1))
    elif isinstance(value, dict):
        for v in value.values():
            refs |= _collect_refs(v)
    elif isinstance(value, (list, tuple)):
        for v in value:
            refs |= _collect_refs(v)
    return refs


class _DictStore:
    """In-memory ``SpecKVStore`` over ``{key: json_string}`` prefetched values.

    Satisfies the ``SpecKVStore`` Protocol structurally (``has``/``get``/
    ``set``). Backs seeding so the synchronous ``[key]`` resolution never
    touches Prefect's async Variable client mid-validation.
    """

    def __init__(self, values: dict[str, str]) -> None:
        self._values = dict(values)

    def has(self, key: str) -> bool:
        return key in self._values

    def get(self, key: str) -> str:
        return self._values[key]

    def set(self, key: str, value: str) -> None:
        self._values[key] = value


def _match_key_ref(value: Any) -> str | None:
    """Return the inner key if ``value`` is a whole-string ``[key]`` reference."""
    if not isinstance(value, str):
        return None
    m = _KEY_REF_RE.fullmatch(value)
    return m.group(1) if m is not None else None


def _map_state_keys_to_vars(
    states: dict[str, dict[str, Any]],
) -> dict[str, str]:
    """Map each speculation-state key ``{func}:{param_index}`` to its Variable.

    jkent stores each state row's raw seed value as ``seed_value_json``; for a
    persisted cursor that raw value is the ``"[key]"`` reference string itself,
    so the mapping is read directly off the rows. States seeded with a literal
    range (no ``[key]``) simply don't map — their cursors aren't ours to
    advance.
    """
    mapping: dict[str, str] = {}
    for state_key, state in states.items():
        raw = state.get("seed_value_json")
        if not raw:
            continue
        try:
            seed_value = json.loads(raw)
        except (ValueError, TypeError):
            continue
        key = _match_key_ref(seed_value)
        if key is not None:
            mapping[state_key] = key
    return mapping


def _compute_next_cursors(
    states: dict[str, dict[str, Any]],
    state_key_to_var: dict[str, str],
) -> dict[str, dict[str, Any]]:
    """Compute the next-run cursor value to write to each referenced Variable.

    For every speculation-state row that maps to a Variable, the next start is
    ``max(seeded_min, highest_successful_id + 1)``: advance past the highest ID
    that succeeded, but never *below* the min the run was seeded with — so a run
    that made no new hits (``highest_successful_id == 0``) holds its cursor
    instead of resetting to 1. ``min`` and ``soft_max`` are both set to that
    value (the ``min == soft_max`` advance-window cursor convention); all other
    template fields (``court_id``, ``year``, ``gap``, ``should_advance``) carry
    through. If several state rows share one Variable, the furthest wins.

    Returns ``{variable_key: new_template_value}``.
    """
    next_by_var: dict[str, dict[str, Any]] = {}
    next_min_by_var: dict[str, int] = {}
    for state_key, state in states.items():
        var_key = state_key_to_var.get(state_key)
        if var_key is None or not state.get("template_json"):
            continue
        template = json.loads(state["template_json"])
        seeded_min = int(template.get("min", 0))
        highest = int(state["highest_successful_id"])
        next_min = max(seeded_min, highest + 1)
        if next_min <= next_min_by_var.get(var_key, -1):
            continue
        next_min_by_var[var_key] = next_min
        next_by_var[var_key] = {**template, "min": next_min, "soft_max": next_min}
    return next_by_var


async def _prefetch(keys: set[str]) -> dict[str, str]:
    """Load the given Prefect Variables into a ``{key: json_string}`` map.

    Absent Variables are omitted; a ``[key]`` with no Variable then fails
    resolution loudly (``SpecKVStore has no value for key``) rather than
    silently seeding nothing.
    """
    from prefect.variables import Variable

    values: dict[str, str] = {}
    for key in keys:
        value = await Variable.aget(key)
        if value is not None:
            values[key] = json.dumps(value)
    return values


async def load_seed_store(
    seed_params: list[dict[str, dict[str, Any]]] | None,
):
    """Return a ``SpecKVStore`` prefetched with the Variables ``seed_params`` cite.

    Returns ``None`` when ``seed_params`` reference no ``[key]`` cursors, so the
    caller can skip entering a ``spec_kv_store`` block entirely.
    """
    keys = variable_keys_in(seed_params)
    if not keys:
        return None
    return _DictStore(await _prefetch(keys))


def scheduled_anchor_date() -> date:
    """The date this run was *scheduled* to fire, for date-shorthand anchoring.

    ``InferrableDateRange`` shorthands (e.g. ``-4d``) resolve relative to this,
    so a run that a worker picks up late still scrapes the window it was meant
    to. Falls back to today only if the runtime has no scheduled time.
    """
    import prefect.runtime

    scheduled = prefect.runtime.flow_run.scheduled_start_time
    return scheduled.date() if scheduled is not None else date.today()


async def advance_speculative_cursors(
    db_path: Path,
    log: Any,
) -> dict[str, dict[str, Any]]:
    """Advance each referenced Prefect Variable to the next run's start cursor.

    Reads the run's speculation state from ``db_path``, maps each speculative
    template back to the Variable it was seeded from (via the persisted
    ``seed_value_json``), and for each writes back ``min = soft_max =
    max(seeded_min, highest_successful_id + 1)`` (so a run that made no new
    hits holds its cursor instead of resetting). All other template fields —
    ``court_id``, ``year``, ``gap``, ``should_advance`` — are preserved.

    Best-effort: any failure is logged and swallowed so cursor bookkeeping never
    fails an otherwise-successful scrape. Returns the ``{key: written_value}``
    map (empty if nothing was advanced), primarily for logging/tests.

    Args:
        db_path: The finished run's SQLite database.
        log: A logger exposing ``.info`` / ``.warning``.
    """
    try:
        return await _advance_speculative_cursors(db_path, log)
    except Exception as exc:  # noqa: BLE001 - bookkeeping must never fail a run
        log.warning("Speculative cursor advancement failed (skipped): %s", exc)
        return {}


async def _advance_speculative_cursors(
    db_path: Path,
    log: Any,
) -> dict[str, dict[str, Any]]:
    from prefect.variables import Variable

    from jkent.driver.database_engine.sql_manager import SQLManager

    async with SQLManager.open(db_path) as db:
        states = await db.load_all_speculation_states()

    if not states:
        return {}

    state_key_to_var = _map_state_keys_to_vars(states)
    next_by_var = _compute_next_cursors(states, state_key_to_var)

    for var_key, value in next_by_var.items():
        await Variable.aset(var_key, value, overwrite=True)
        log.info(
            "Advanced speculative cursor %r -> min=%d", var_key, value["min"]
        )
    return next_by_var
