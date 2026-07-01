"""JKent scraper discovery and schema-name derivation.

``scraper_schema_name`` is a pure function used by the Pulumi program to name
each scraper's deployment and work queue, and bound as the ``scraper_schema``
parameter the scrape flow receives (where it becomes the S3 key prefix).
Keeping it in one place guarantees those uses agree.

``discover_scraper_paths`` walks the ``juriscraper`` package for JKent
``BaseScraper`` subclasses; only the Pulumi program calls it, so its heavy
imports (juriscraper, jkent) are deferred into the function body.
"""

from __future__ import annotations

import re


def scraper_schema_name(scraper_path: str) -> str:
    """Canonical schema/namespace slug for a scraper.

    Derived from the class name with any trailing ``Scraper`` removed and the
    remainder converted to ``snake_case`` (e.g. ``ArkansasAppellateScraper`` ->
    ``arkansas_appellate``). Used as the S3 key prefix, the deployment name, and
    the work-queue name, so all three always agree.

    Args:
        scraper_path: ``"module.path:ClassName"`` import path (the same value
            the ``scraper-run`` flow receives as ``scraper_path``).
    """
    class_name = scraper_path.rpartition(":")[2].rpartition(".")[2]
    name = re.sub(r"Scraper$", "", class_name)
    # Split camel/Pascal case into words, then collapse to snake_case.
    name = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name)
    name = re.sub(r"(?<=[A-Z])(?=[A-Z][a-z])", "_", name)
    return re.sub(r"_+", "_", name).strip("_").lower()


def scraper_court_ids(scraper_path: str) -> list[str]:
    """Return the CourtListener court ids a scraper covers, sorted.

    Reads the scraper class's ``court_ids`` attribute (a set; empty if the
    scraper doesn't declare any). Used by the Pulumi program to tag each
    scraper's deployment with the courts it covers. Imports the scraper module,
    so (like ``discover_scraper_paths``) it pulls in juriscraper/jkent.

    Args:
        scraper_path: ``"module.path:ClassName"`` import path.
    """
    import importlib

    module_path, _, qualname = scraper_path.partition(":")
    obj: object = importlib.import_module(module_path)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return sorted(getattr(obj, "court_ids", None) or ())


def scraper_needs_browser(scraper_path: str) -> bool:
    """Whether a scraper requires a live browser transport.

    Reuses jkent's transport-selection predicate (``needs_browser`` reads the
    class's ``driver_requirements`` against ``BROWSER_REQUIREMENTS``) so worker
    routing agrees with what the driver actually does at run time. The Pulumi
    program uses this to send browser scrapers to the ``browser-pool`` (whose
    worker has a browser engine installed and runs one scrape at a time) and
    everything else to the lean HTTP ``scraper-pool``.

    The predicate reads a ClassVar, so the scraper is never instantiated.

    Args:
        scraper_path: ``"module.path:ClassName"`` import path.
    """
    import importlib

    from jkent.driver.unified_driver.bootstrap import needs_browser

    module_path, _, qualname = scraper_path.partition(":")
    obj: object = importlib.import_module(module_path)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return needs_browser(obj)


def discover_scraper_paths(package: str = "juriscraper") -> list[str]:
    """Return ``module:ClassName`` paths for every JKent scraper in *package*.

    Walks all submodules of *package*, importing each (failures are skipped),
    and collects classes that subclass ``jkent.data_types.BaseScraper``.
    A class is recorded only in the module where it is defined, so re-exports
    don't produce duplicates. The result is sorted for deterministic output.
    """
    import importlib
    import pkgutil

    from jkent.data_types import BaseScraper

    def _is_test_module(name: str) -> bool:
        """True for test modules/packages we don't want to provision."""
        return any(
            part in ("test", "tests", "conftest") or part.startswith("test_")
            for part in name.split(".")
        )

    root = importlib.import_module(package)
    paths: set[str] = set()

    for mod_info in pkgutil.walk_packages(
        root.__path__, prefix=f"{root.__name__}.", onerror=lambda _name: None
    ):
        if _is_test_module(mod_info.name):
            continue
        try:
            module = importlib.import_module(mod_info.name)
        except Exception:
            # Legacy/optional modules may fail to import; they hold no JKent
            # scrapers we care about, so skip them.
            continue
        for obj in vars(module).values():
            if (
                isinstance(obj, type)
                and issubclass(obj, BaseScraper)
                and obj is not BaseScraper
                and obj.__module__ == mod_info.name
            ):
                paths.add(f"{obj.__module__}:{obj.__qualname__}")

    return sorted(paths)
