"""Byte-range partial GRIB downloader.

Given a list of :class:`ByteRange` objects (typically produced by
:func:`climate_download.grib.index.merge_ranges`), this module fetches each
range with an HTTP ``Range`` request and writes the bytes into a local file.
The output file ends up as a valid GRIB document: GRIB messages are
self-delimiting, so concatenating a subset of them in original order yields a
file ``cfgrib`` / ``wgrib2`` can read directly.

The implementation is intentionally synchronous and uses a thread pool for
concurrency. This keeps the public surface simple, plays well with ``cron``,
and avoids forcing async on the rest of the codebase.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

import httpx
import structlog
from tenacity import (
    RetryError,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from climate_download.grib.index import ByteRange

__all__ = ["PartialDownloadError", "PartialDownloader"]

_log = structlog.get_logger(__name__)

# HTTP statuses for which a retry has any chance of succeeding.
_RETRYABLE_STATUSES = frozenset({408, 425, 429, 500, 502, 503, 504})


class PartialDownloadError(RuntimeError):
    """Raised when a byte-range request ultimately fails."""


class _RetryableHTTPError(Exception):
    """Internal wrapper so tenacity only retries the responses we want."""


@dataclass(slots=True)
class _FetchResult:
    range: ByteRange
    payload: bytes


class PartialDownloader:
    """Download a subset of a remote GRIB file using HTTP ``Range`` requests.

    Parameters
    ----------
    client:
        Optional pre-built ``httpx.Client``. When omitted, a client with the
        configured timeout is created and closed by :meth:`close` /
        ``__exit__``.
    timeout:
        Per-request timeout in seconds (only used when ``client`` is None).
    max_workers:
        Thread pool size for concurrent range fetches.
    max_attempts:
        Number of attempts per range (initial + retries) for transient errors.
    """

    def __init__(
        self,
        client: httpx.Client | None = None,
        *,
        timeout: float = 60.0,
        max_workers: int = 4,
        max_attempts: int = 4,
        progress: bool = False,
    ) -> None:
        if max_workers < 1:
            raise ValueError("max_workers must be >= 1")
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        self._owns_client = client is None
        self._client = client or httpx.Client(timeout=timeout)
        self._max_workers = max_workers
        self._max_attempts = max_attempts
        self._progress = progress

    def __enter__(self) -> "PartialDownloader":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def download(
        self,
        url: str,
        ranges: Sequence[ByteRange],
        output_path: str | Path,
    ) -> int:
        """Fetch ``ranges`` from ``url`` and write them into ``output_path``.

        Bytes are written in offset order so the resulting file is a valid
        concatenation of GRIB messages. Returns the total number of bytes
        written.
        """
        if not ranges:
            raise ValueError("ranges must not be empty")
        out = Path(output_path)
        out.parent.mkdir(parents=True, exist_ok=True)

        ordered = sorted(ranges, key=lambda r: r.start)
        total = sum(r.length for r in ordered)
        _log.info(
            "partial_download_start",
            url=url,
            ranges=len(ordered),
            bytes=total,
            workers=self._max_workers,
        )

        results: dict[int, bytes] = {}
        with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
            futures = {pool.submit(self._fetch_one, url, r): r for r in ordered}
            iterator: object = as_completed(futures)
            if self._progress:
                try:
                    from tqdm import tqdm
                    iterator = tqdm(
                        as_completed(futures),
                        total=len(ordered),
                        desc=Path(output_path).name,
                        unit="rng", leave=False,
                    )
                except ImportError:  # pragma: no cover - tqdm is a base dep
                    _log.warning("tqdm_unavailable")
            for fut in iterator:  # type: ignore[assignment]
                res = fut.result()
                results[res.range.start] = res.payload

        written = 0
        with out.open("wb") as fh:
            for r in ordered:
                payload = results[r.start]
                fh.write(payload)
                written += len(payload)

        _log.info("partial_download_done", url=url, bytes=written, path=str(out))
        return written

    def _fetch_one(self, url: str, byte_range: ByteRange) -> _FetchResult:
        attempt = retry(
            reraise=True,
            stop=stop_after_attempt(self._max_attempts),
            wait=wait_exponential(multiplier=0.5, min=0.5, max=10.0),
            retry=retry_if_exception_type(
                (_RetryableHTTPError, httpx.TransportError, httpx.TimeoutException)
            ),
        )(self._fetch_once)
        try:
            payload = attempt(url, byte_range)
        except RetryError as exc:  # pragma: no cover - reraise=True bypasses this
            raise PartialDownloadError(str(exc)) from exc
        except (_RetryableHTTPError, httpx.HTTPError) as exc:
            raise PartialDownloadError(
                f"failed to fetch {byte_range.http_header()} from {url}: {exc}"
            ) from exc
        return _FetchResult(range=byte_range, payload=payload)

    def _fetch_once(self, url: str, byte_range: ByteRange) -> bytes:
        headers = {"Range": byte_range.http_header()}
        resp = self._client.get(url, headers=headers)
        if resp.status_code in _RETRYABLE_STATUSES:
            raise _RetryableHTTPError(
                f"retryable status {resp.status_code} for {byte_range.http_header()}"
            )
        if resp.status_code not in (200, 206):
            raise PartialDownloadError(
                f"unexpected status {resp.status_code} for {byte_range.http_header()}"
            )
        payload = resp.content
        if len(payload) != byte_range.length:
            raise PartialDownloadError(
                f"short read for {byte_range.http_header()}: "
                f"got {len(payload)} bytes, expected {byte_range.length}"
            )
        return payload
