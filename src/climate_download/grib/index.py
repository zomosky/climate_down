"""Parse GRIB sidecar index files and select message byte ranges.

Two on-the-wire formats are supported:

* **ECMWF JSON-Lines** (``.index``) — one JSON object per GRIB message with
  ``_offset`` / ``_length`` fields. Used by AIFS / IFS open-data on GCS.
* **wgrib2 colon-separated text** (``.idx``) — ``msg:offset:date:param:level:fcst:``
  with no length field. Used by NOAA GFS on AWS S3 (and most NCEP products).
  The length of message ``i`` is ``offset[i+1] - offset[i]``; the last
  message's length is ``total_size - offset[-1]``, so callers must pass the
  GRIB file size in (typically from a HEAD ``Content-Length``).

Both parsers return a list of :class:`IndexRecord` so the rest of the
pipeline (``IndexFilter`` / ``merge_ranges`` / ``PartialDownloader``) is
source-agnostic.
"""

from __future__ import annotations

import json
import re
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

__all__ = [
    "ByteRange",
    "IndexFilter",
    "IndexRecord",
    "filter_records",
    "merge_ranges",
    "parse_index",
    "parse_index_text",
    "parse_wgrib2_idx_text",
]


class IndexRecord(BaseModel):
    """One GRIB message entry from a ``.index`` file."""

    model_config = ConfigDict(extra="allow", frozen=True)

    param: str
    levtype: str
    levelist: str | None = None
    level_desc: str | None = None
    step: str | None = None
    date: str | None = None
    time: str | None = None
    offset: int = Field(alias="_offset")
    length: int = Field(alias="_length")

    @property
    def end(self) -> int:
        """Exclusive end offset of the message."""
        return self.offset + self.length


class ByteRange(BaseModel):
    """A contiguous ``[start, end)`` byte slice covering one or more messages."""

    model_config = ConfigDict(frozen=True)

    start: int
    end: int

    @property
    def length(self) -> int:
        return self.end - self.start

    def http_header(self) -> str:
        """Render as an HTTP ``Range`` header value (inclusive end)."""
        return f"bytes={self.start}-{self.end - 1}"


class IndexFilter(BaseModel):
    """Selection predicate used by :func:`filter_records`.

    Empty fields are treated as wildcards. ``levels`` matches the string form
    of ``levelist`` so callers can pass ``["850", "925"]`` directly.
    """

    model_config = ConfigDict(frozen=True)

    params: Sequence[str] | None = None
    levtypes: Sequence[str] | None = None
    levels: Sequence[str] | None = None
    steps: Sequence[str] | None = None

    def matches(self, record: IndexRecord) -> bool:
        if self.params is not None and record.param not in self.params:
            return False
        if self.levtypes is not None and record.levtype not in self.levtypes:
            return False
        if self.levels is not None:
            if record.levelist is None or record.levelist not in self.levels:
                return False
        if self.steps is not None:
            if record.step is None or record.step not in self.steps:
                return False
        return True


def _coerce(raw: dict[str, Any]) -> IndexRecord:
    # ``levelist`` is sometimes absent (surface fields); pydantic handles None.
    if "levelist" in raw and raw["levelist"] is not None:
        raw = {**raw, "levelist": str(raw["levelist"])}
    if "step" in raw and raw["step"] is not None:
        raw = {**raw, "step": str(raw["step"])}
    return IndexRecord.model_validate(raw)


def parse_index_text(text: str) -> list[IndexRecord]:
    """Parse the in-memory contents of a ``.index`` file."""
    records: list[IndexRecord] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.strip()
        if not stripped:
            continue
        try:
            raw = json.loads(stripped)
        except json.JSONDecodeError as exc:
            raise ValueError(f"invalid JSON on line {lineno}: {exc}") from exc
        records.append(_coerce(raw))
    return records


def parse_index(path: str | Path) -> list[IndexRecord]:
    """Read and parse a ``.index`` file from disk."""
    return parse_index_text(Path(path).read_text(encoding="utf-8"))


def filter_records(
    records: Iterable[IndexRecord], selector: IndexFilter
) -> list[IndexRecord]:
    """Return the subset of ``records`` matching ``selector``, preserving order."""
    return [r for r in records if selector.matches(r)]


def merge_ranges(
    records: Iterable[IndexRecord], gap_tolerance: int = 0
) -> list[ByteRange]:
    """Merge contiguous (or near-contiguous) message slices into byte ranges.

    Records are first sorted by ``offset``. Two slices are merged when the gap
    between them is at most ``gap_tolerance`` bytes. Setting ``gap_tolerance``
    to a few KiB lets callers trade a small amount of wasted bandwidth for far
    fewer HTTP round-trips when the wanted messages are dense.
    """
    if gap_tolerance < 0:
        raise ValueError("gap_tolerance must be non-negative")
    ordered = sorted(records, key=lambda r: r.offset)
    merged: list[ByteRange] = []
    for rec in ordered:
        if not merged:
            merged.append(ByteRange(start=rec.offset, end=rec.end))
            continue
        last = merged[-1]
        if rec.offset <= last.end + gap_tolerance:
            if rec.end > last.end:
                merged[-1] = ByteRange(start=last.start, end=rec.end)
        else:
            merged.append(ByteRange(start=rec.offset, end=rec.end))
    return merged



# --- wgrib2 .idx (NOAA GFS / NCEP) -----------------------------------------

_WGRIB2_PL = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\s+mb$")
_WGRIB2_HAG = re.compile(r"^([0-9]+(?:\.[0-9]+)?)\s+m\s+above\s+ground$")
_WGRIB2_HBG = re.compile(r"^([0-9.]+)-([0-9.]+)\s+m\s+below\s+ground$")
_WGRIB2_FCST_H = re.compile(
    r"^(?:(\d+)-)?(\d+)\s+hour\s+"
    r"(?:fcst|ave\s+fcst|acc\s+fcst|max\s+fcst|min\s+fcst)$"
)


def _classify_wgrib2_level(level: str) -> tuple[str, str | None]:
    """Map a wgrib2 level descriptor to ``(levtype, levelist)``.

    Categories chosen to keep ``VariableGroup.levtype`` filtering meaningful
    across sources:

    * ``pl``  — isobaric, e.g. ``"500 mb"``
    * ``hag`` — height above ground, e.g. ``"10 m above ground"``
    * ``hbg`` — soil layer below ground, e.g. ``"0-0.1 m below ground"``
    * ``atm`` — column / mean-sea-level / tropopause / boundary-layer summaries
    * ``sfc`` — plain surface
    * ``other`` — anything else (PV surface, hybrid level, sigma, ...)
    """
    s = level.strip()
    if (m := _WGRIB2_PL.match(s)):
        return "pl", m.group(1)
    if (m := _WGRIB2_HAG.match(s)):
        return "hag", m.group(1)
    if (m := _WGRIB2_HBG.match(s)):
        return "hbg", f"{m.group(1)}-{m.group(2)}"
    if s == "surface":
        return "sfc", None
    if s in ("mean sea level", "entire atmosphere",
            "entire atmosphere (considered as a single layer)",
            "tropopause", "max wind", "planetary boundary layer"):
        return "atm", None
    return "other", None


def _classify_wgrib2_fcst(fcst: str) -> str | None:
    """Extract the integer end-of-window forecast hour, ``None`` for analyses."""
    s = fcst.strip()
    if s in ("anl", "analysis"):
        return "0"
    if (m := _WGRIB2_FCST_H.match(s)):
        return m.group(2)
    return None


def parse_wgrib2_idx_text(text: str, *, total_size: int) -> list[IndexRecord]:
    """Parse a wgrib2-style ``.idx`` file (NOAA GFS / most NCEP products).

    ``total_size`` is the byte length of the companion GRIB file; it is needed
    because wgrib2 idx files store only offsets, so the last message's length
    is ``total_size - last_offset``. Callers typically obtain it from a HEAD
    ``Content-Length`` on the GRIB URL.
    """
    if total_size <= 0:
        raise ValueError("total_size must be positive (in bytes)")

    rows: list[tuple[int, str, str, str]] = []  # (offset, param, level, fcst)
    for lineno, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s:
            continue
        parts = s.split(":")
        # Format: msg : offset : d=YYYYMMDDHH : param : level : fcst : (trailing colon may add empty)
        if len(parts) < 6:
            raise ValueError(f"wgrib2 idx line {lineno}: expected >=6 fields, got {len(parts)}")
        try:
            offset = int(parts[1])
        except ValueError as exc:
            raise ValueError(f"wgrib2 idx line {lineno}: bad offset {parts[1]!r}") from exc
        rows.append((offset, parts[3], parts[4], parts[5]))

    if not rows:
        return []

    records: list[IndexRecord] = []
    for i, (offset, param, level, fcst) in enumerate(rows):
        next_off = rows[i + 1][0] if i + 1 < len(rows) else total_size
        length = next_off - offset
        if length <= 0:
            raise ValueError(
                f"wgrib2 idx: non-monotonic offsets near message {i + 1} "
                f"(offset={offset}, next={next_off})"
            )
        levtype, levelist = _classify_wgrib2_level(level)
        records.append(
            IndexRecord.model_validate({
                "param": param,
                "levtype": levtype,
                "levelist": levelist,
                "level_desc": level.strip(),
                "step": _classify_wgrib2_fcst(fcst),
                "_offset": offset,
                "_length": length,
            })
        )
    return records
