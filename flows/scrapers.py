"""JKent scraper discovery and concurrency-limit naming.

``scraper_limit_name`` is a pure function shared by the scrape flow (which
*acquires* a scraper's global concurrency limit at runtime) and the Pulumi
program (which *creates* one limit per scraper). Keeping it in one place
guarantees the two sides agree on the name.

``discover_scraper_paths`` walks the ``juriscraper`` package for JKent
``BaseScraper`` subclasses; only the Pulumi program calls it, so its heavy
imports (juriscraper, jkent) are deferred into the function body.
"""

from __future__ import annotations

# Prefix for the per-scraper global concurrency limit names.
LIMIT_PREFIX = "scraper:"


def scraper_limit_name(scraper_path: str) -> str:
    """Canonical global-concurrency-limit name for a scraper.

    Args:
        scraper_path: ``"module.path:ClassName"`` import path (the same value
            the ``scraper-run`` flow receives as ``scraper_path``).
    """
    return f"{LIMIT_PREFIX}{scraper_path}"


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
