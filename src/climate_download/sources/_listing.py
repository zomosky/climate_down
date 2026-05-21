"""Bucket-LIST helpers for step / variable / init discovery.

Public buckets behind every built-in source expose an XML LIST endpoint that
is *roughly* S3-compatible but differs in three nuisance details:

============ ===================================== ==================== ==================
backend      example host                          pagination param     bucket location
============ ===================================== ==================== ==================
``s3v2``     ``noaa-gfs-bdp-pds.s3.amazonaws.com`` ``continuation-token`` host subdomain
``gcs``      ``storage.googleapis.com``            ``marker``             first path segment
============ ===================================== ==================== ==================

Both return ``<ListBucketResult>`` documents whose XML namespaces also differ
(`http://s3.amazonaws.com/doc/2006-03-01/` vs ``http://doc.s3.amazonaws.com/2006-03-01``),
so we strip namespaces before walking the tree.

``list_remote_steps`` is the user-facing helper: render the source's index
URL template with the real ``(date, cycle)``, leave ``{step…}`` as a marker,
derive the listing prefix + key regex, page through results, return the
sorted unique step ints. ``None`` means "this template doesn't map to a
known backend"; the caller falls back to a static candidate range.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import urlparse
from xml.etree import ElementTree as ET

import httpx

from climate_download.sources._http import request_with_retry

__all__ = ["list_remote_steps", "list_s3_steps"]

_STEP_PLACEHOLDER = re.compile(r"\{step(?::[^}]+)?\}")
_MARKER = "__CLIMATE_DOWNLOAD_STEP__"


@dataclass(frozen=True, slots=True)
class _Backend:
    list_url: str           # endpoint to GET (without query)
    key_prefix: str         # prefix to pass to ?prefix=
    pagination: str         # "continuation-token" or "marker"
    list_type_v2: bool      # send ?list-type=2 (S3) or not (GCS)


def _render_template(template: str, *, date: str, cycle: int) -> str:
    masked = _STEP_PLACEHOLDER.sub(_MARKER, template)
    return masked.format(date=date, cycle=cycle)


def _detect_backend(rendered: str) -> tuple[_Backend, re.Pattern[str]] | None:
    """Inspect the rendered URL, return (Backend, basename-regex) or ``None``.

    The basename regex captures the integer step value; it is matched against
    every ``<Key>`` text after stripping the listing prefix.
    """
    parsed = urlparse(rendered)
    if not parsed.scheme or not parsed.netloc:
        return None
    key = parsed.path.lstrip("/")
    if _MARKER not in key:
        return None

    host = parsed.netloc
    scheme = parsed.scheme
    # GCS: bucket lives in path; LIST endpoint is host + '/' + bucket + '/'.
    if host == "storage.googleapis.com":
        bucket, _, rest = key.partition("/")
        if not bucket or _MARKER not in rest:
            return None
        head, _, _tail = rest.rpartition("/")
        prefix = (head + "/") if head else ""
        basename = rest[len(head) + 1:] if head else rest
        backend = _Backend(
            list_url=f"{scheme}://{host}/{bucket}/",
            key_prefix=prefix,
            pagination="marker",
            list_type_v2=False,
        )
    # S3 v2: bucket is the host subdomain; LIST endpoint is host + '/'.
    elif host.endswith(".s3.amazonaws.com") or ".s3." in host:
        head, _, _tail = key.rpartition("/")
        prefix = (head + "/") if head else ""
        basename = key[len(head) + 1:] if head else key
        backend = _Backend(
            list_url=f"{scheme}://{host}/",
            key_prefix=prefix,
            pagination="continuation-token",
            list_type_v2=True,
        )
    else:
        return None

    parts = basename.split(_MARKER)
    if len(parts) < 2:
        return None
    basename_regex = r"(\d+)".join(re.escape(p) for p in parts)
    return backend, re.compile(rf"^{basename_regex}$")


def _strip_ns(root: ET.Element) -> None:
    for el in root.iter():
        if "}" in el.tag:
            el.tag = el.tag.split("}", 1)[1]


def list_remote_steps(
    client: httpx.Client,
    *,
    index_url_template: str,
    date: str,
    cycle: int,
    max_pages: int = 32,
) -> list[int] | None:
    """Return the sorted unique step values published in the bucket prefix.

    ``None`` is returned when the template host is unknown or has no
    ``{step…}`` placeholder; callers should then fall back to a static range.
    """
    rendered = _render_template(index_url_template, date=date, cycle=cycle)
    detected = _detect_backend(rendered)
    if detected is None:
        return None
    backend, basename_regex = detected

    steps: set[int] = set()
    token: str | None = None
    for _ in range(max_pages):
        params: dict[str, str] = {"prefix": backend.key_prefix}
        if backend.list_type_v2:
            params["list-type"] = "2"
        if token is not None:
            params[backend.pagination] = token
        resp = request_with_retry(client, "GET", backend.list_url, params=params)
        resp.raise_for_status()
        root = ET.fromstring(resp.text)
        _strip_ns(root)
        for content in root.iterfind("Contents"):
            key_el = content.find("Key")
            if key_el is None or key_el.text is None:
                continue
            tail = key_el.text[len(backend.key_prefix):]
            if (m := basename_regex.match(tail)):
                steps.add(int(m.group(1)))
        truncated = (root.findtext("IsTruncated") or "false").lower() == "true"
        if not truncated:
            break
        next_tag = (
            "NextContinuationToken" if backend.pagination == "continuation-token"
            else "NextMarker"
        )
        token = root.findtext(next_tag)
        if not token:
            break
    return sorted(steps)


# Backward-compatible alias for callers that imported the old name.
list_s3_steps = list_remote_steps
