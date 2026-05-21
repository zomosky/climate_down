"""Shared HTTP retry wrapper used by every source adapter.

All metadata-shaped requests (probe HEAD, sidecar GET, bucket LIST GET) flow
through :func:`request_with_retry` so a single transient network failure
cannot collapse a long-running job. Byte-range fetches for the actual GRIB
payload go through :class:`PartialDownloader`'s own tenacity loop; both code
paths share the same ``max_attempts=4`` default and the same retry trigger
set (TransportError / TimeoutException / 408 / 425 / 429 / 5xx), so probe,
sidecar fetch, listing and byte-range download all have the same resilience
floor.
"""

from __future__ import annotations

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

__all__ = ["TRANSIENT_HTTP_EXCEPTIONS", "request_with_retry"]

_log = structlog.get_logger(__name__)

# httpx.TransportError covers ConnectError / ReadError / RemoteProtocolError
# / WriteError / NetworkError / ProxyError; TimeoutException covers the
# connect / read / write / pool timeout family.
TRANSIENT_HTTP_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.TransportError, httpx.TimeoutException,
)

_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class _RetryableStatus(Exception):
    """Internal marker so tenacity can retry transient 5xx/429 responses."""


def request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    max_attempts: int = 4,
    **kwargs: object,
) -> httpx.Response:
    """Perform one HTTP request, retrying on transient failures.

    Retries on:

    * Connect / read / write / protocol errors (``httpx.TransportError``)
    * Timeouts (``httpx.TimeoutException``)
    * Responses with status 408 / 425 / 429 / 5xx

    All other responses (200, 206, 404, 403, ...) are returned as-is; the
    caller decides what to do with them (e.g. ``probe_step`` treats 404 as
    "step not published" rather than as an error).
    """
    @retry(
        reraise=True,
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=10.0),
        retry=retry_if_exception_type(
            TRANSIENT_HTTP_EXCEPTIONS + (_RetryableStatus,)
        ),
    )
    def _do() -> httpx.Response:
        try:
            resp = client.request(method, url, **kwargs)  # type: ignore[arg-type]
        except TRANSIENT_HTTP_EXCEPTIONS as exc:
            _log.warning(
                "http_retry", method=method, url=url,
                error=f"{type(exc).__name__}: {exc}",
            )
            raise
        if resp.status_code in _RETRYABLE_STATUSES:
            _log.warning(
                "http_retry", method=method, url=url,
                status=resp.status_code,
            )
            raise _RetryableStatus(f"retryable status {resp.status_code}")
        return resp

    return _do()
