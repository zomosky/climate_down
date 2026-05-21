"""One-call structured logging + 3rd-party noise suppression.

Both example scripts call :func:`configure_logging` so log output stays
JSON-only on stderr and chatty libraries (httpx request lines, cfgrib
``FutureWarning``s) do not bury the events that actually matter.
"""

from __future__ import annotations

import logging
import sys
import warnings
from pathlib import Path
from typing import Iterable

import structlog

__all__ = ["configure_logging"]

# Library loggers we want to keep but quiet down.
_NOISY_LOGGERS: tuple[str, ...] = (
    "httpx",
    "httpcore",
    "httpcore.http11",
    "httpcore.connection",
)


class _TqdmStreamHandler(logging.StreamHandler):
    """``StreamHandler`` that emits via ``tqdm.write`` so log lines do not
    corrupt any active progress bar. When no bar is active ``tqdm.write``
    is equivalent to a plain stream write, so this handler is safe to use
    unconditionally."""

    def emit(self, record: logging.LogRecord) -> None:  # noqa: D401
        try:
            msg = self.format(record)
            try:
                from tqdm import tqdm
            except ImportError:  # pragma: no cover — tqdm is a base dep
                self.stream.write(msg + self.terminator)
                self.flush()
                return
            tqdm.write(msg, file=self.stream)
        except Exception:  # pragma: no cover — defensive
            self.handleError(record)


def configure_logging(
    *,
    level: int = logging.INFO,
    stderr_level: int | None = None,
    quiet_loggers: Iterable[str] = _NOISY_LOGGERS,
    silence_cfgrib_future_warnings: bool = True,
    log_file: str | Path | None = None,
) -> None:
    """Install JSON structlog renderer and silence common noise sources.

    Parameters
    ----------
    level:
        Threshold for structlog events and for the optional ``log_file``
        handler (the file always captures the full audit trail).
    stderr_level:
        Optional stricter threshold for the stderr stream handler. When
        tqdm progress bars are active, callers pass ``logging.WARNING``
        to keep INFO chatter out of the terminal so the bars render
        cleanly; ``log_file`` still receives every event. Defaults to
        ``level`` (no extra filtering).
    quiet_loggers:
        Logger names to clamp to ``WARNING`` (default: httpx + httpcore).
    silence_cfgrib_future_warnings:
        If ``True`` (default), suppress ``cfgrib``'s xarray-merge
        ``FutureWarning`` chatter. Other warnings are untouched.
    log_file:
        Optional path; when given, every structlog/JSON line is also
        appended to this file so a backgrounded run leaves a tailable
        audit trail next to stderr output.
    """
    if stderr_level is None:
        stderr_level = level

    stderr_handler = _TqdmStreamHandler(sys.stderr)
    stderr_handler.setLevel(stderr_level)
    stderr_handler.setFormatter(logging.Formatter("%(message)s"))
    handlers: list[logging.Handler] = [stderr_handler]
    if log_file is not None:
        path = Path(log_file)
        path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setLevel(level)
        file_handler.setFormatter(logging.Formatter("%(message)s"))
        handlers.append(file_handler)

    root = logging.getLogger()
    root.setLevel(min(level, stderr_level))
    for h in list(root.handlers):
        root.removeHandler(h)
    for h in handlers:
        root.addHandler(h)

    for name in quiet_loggers:
        logging.getLogger(name).setLevel(logging.WARNING)

    # Route structlog through stdlib so per-handler levels apply: this
    # lets stderr stay at WARNING (clean tqdm bars) while ``log_file``
    # keeps the full INFO JSON trail.
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
    )

    if silence_cfgrib_future_warnings:
        warnings.filterwarnings("ignore", category=FutureWarning, module=r"cfgrib(\..*)?")
