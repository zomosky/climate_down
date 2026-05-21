"""In-process registry mapping ``type:`` strings to source adapter classes.

Sources self-register via the :func:`register` decorator at import time.
The orchestrator never imports concrete sources directly — it goes through
:func:`get_source` so adding a new file under :mod:`climate_download.sources`
plus an ``__init__.py`` import is enough to wire it in.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, TypeVar

if TYPE_CHECKING:
    from climate_download.sources.base import Source

__all__ = ["get_source", "list_sources", "register", "SOURCE_REGISTRY"]


SOURCE_REGISTRY: dict[str, type["Source"]] = {}

_T = TypeVar("_T", bound=type)


def register(name: str) -> Callable[[_T], _T]:
    """Decorator: register a source class under ``name``.

    Raises ``ValueError`` if ``name`` is already taken so silent overrides
    cannot mask a typo when two source files claim the same type.
    """

    def _decorate(cls: _T) -> _T:
        if name in SOURCE_REGISTRY:
            existing = SOURCE_REGISTRY[name]
            raise ValueError(
                f"source type {name!r} already registered to {existing!r}; "
                f"refusing to overwrite with {cls!r}"
            )
        SOURCE_REGISTRY[name] = cls  # type: ignore[assignment]
        return cls

    return _decorate


def get_source(name: str) -> type["Source"]:
    """Look up the adapter class registered for ``name``."""
    try:
        return SOURCE_REGISTRY[name]
    except KeyError as exc:
        known = ", ".join(sorted(SOURCE_REGISTRY)) or "<none>"
        raise KeyError(
            f"unknown source type {name!r}; registered types: {known}"
        ) from exc


def list_sources() -> list[str]:
    """Return the list of registered source type names, sorted."""
    return sorted(SOURCE_REGISTRY)
