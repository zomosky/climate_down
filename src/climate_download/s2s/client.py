"""Thin wrapper around ``cdsapi.Client`` for the ECDS endpoint.

ECDS uses the same ``cdsapi`` library as Copernicus CDS but with a different
URL (``https://ecds.ecmwf.int/api``) and a separate ``~/.ecdsapirc`` file.
Credentials are loaded from disk only — never from environment variables or
function arguments — so secrets cannot leak into shell history, process
listings or test fixtures.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

__all__ = [
    "ECDSClientFactory",
    "ECDSCredentials",
    "default_credentials_path",
    "load_credentials",
    "make_client",
]


_DEFAULT_CRED_FILE = ".ecdsapirc"
_DEFAULT_URL = "https://ecds.ecmwf.int/api"


@dataclass(frozen=True, slots=True)
class ECDSCredentials:
    """Credentials read from ``~/.ecdsapirc`` (yaml-ish ``key: value`` pairs).

    ``key`` is intentionally not stringified into ``__repr__`` to keep tokens
    out of accidental log lines; only the URL is exposed.
    """

    url: str
    key: str

    def __repr__(self) -> str:  # pragma: no cover — defensive only
        return f"ECDSCredentials(url={self.url!r}, key=<redacted>)"


@runtime_checkable
class ECDSClientLike(Protocol):
    """Minimal subset of ``cdsapi.Client`` we depend on."""

    def retrieve(self, name: str, request: dict[str, Any], target: str) -> Any: ...


def default_credentials_path() -> Path:
    """Return the canonical credentials path: ``$HOME/.ecdsapirc``."""
    return Path.home() / _DEFAULT_CRED_FILE


def load_credentials(path: Path | None = None) -> ECDSCredentials:
    """Parse ``url:`` and ``key:`` lines from a ``.ecdsapirc``-style file.

    The format mirrors ``~/.cdsapirc``: yaml-ish single-level mapping with
    one ``url:`` line and one ``key:`` line. Trailing comments and blank
    lines are ignored. Any other line is rejected so a stray ``email:`` (a
    common Web-API confusion) surfaces immediately rather than being
    silently dropped.
    """
    src = path or default_credentials_path()
    if not src.is_file():
        raise FileNotFoundError(
            f"ECDS credentials file not found: {src}. "
            f"Create it with two lines:\n"
            f"  url: {_DEFAULT_URL}\n"
            f"  key: <your token from https://ecds.ecmwf.int/how-to-api>\n"
            f"then run: chmod 600 {src}"
        )
    url = key = ""
    for raw in src.read_text(encoding="utf-8").splitlines():
        line = raw.split("#", 1)[0].strip()
        if not line:
            continue
        if ":" not in line:
            raise ValueError(f"{src}: malformed line (expected 'name: value'): {raw!r}")
        name, _, val = line.partition(":")
        name = name.strip().lower()
        val = val.strip()
        if name == "url":
            url = val
        elif name == "key":
            key = val
        else:
            raise ValueError(
                f"{src}: unexpected key {name!r}; ECDS only accepts 'url' and 'key'. "
                f"Did you mix up ~/.ecmwfapirc (Web API) with ~/.ecdsapirc (ECDS)?"
            )
    if not url or not key:
        raise ValueError(f"{src}: both 'url' and 'key' are required")
    return ECDSCredentials(url=url, key=key)


@runtime_checkable
class ECDSClientFactory(Protocol):
    """Callable that returns an ECDS-compatible client.

    Tests inject a stub factory to avoid hitting the real ECDS service.
    """

    def __call__(self, *, url: str, key: str, quiet: bool = False) -> ECDSClientLike: ...


def _default_factory(*, url: str, key: str, quiet: bool = False) -> ECDSClientLike:
    """Construct a real ``cdsapi.Client`` against the ECDS endpoint.

    Imports are deferred so the rest of the package remains usable when the
    optional ``[s2s]`` extra is not installed; only the orchestrator needs
    cdsapi.
    """
    try:
        import cdsapi  # type: ignore
    except ImportError as exc:  # pragma: no cover — surfaced to user
        raise ImportError(
            "cdsapi is required for S2S downloads. "
            "Install with: uv sync --extra s2s  (or: pip install 'climate-download[s2s]')"
        ) from exc
    return cdsapi.Client(url=url, key=key, quiet=quiet, progress=False)


def make_client(
    credentials: ECDSCredentials | None = None,
    *,
    quiet: bool = True,
    factory: ECDSClientFactory | None = None,
) -> ECDSClientLike:
    """Build an ECDS client, deferring the heavy import until called.

    ``factory`` exists for tests; production code passes ``credentials`` (or
    ``None`` to load from ``~/.ecdsapirc``).
    """
    creds = credentials or load_credentials()
    fn = factory or _default_factory
    return fn(url=creds.url, key=creds.key, quiet=quiet)
