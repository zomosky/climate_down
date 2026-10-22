"""Shared validation of ECMWF indexes concatenated from em/ep/es blocks."""
from __future__ import annotations

from climate_download.grib.index import IndexRecord


def normalize_offsets(records: list[IndexRecord], object_size: int) -> tuple[list[IndexRecord], str]:
    """Repair reset offsets only when all message lengths cover the GRIB object.

    A normal global index may have gaps and must keep its original offsets.
    Backwards or overlapping offsets identify the concatenated-block format.
    """
    if any(r.length < 16 or r.offset < 0 for r in records):
        raise ValueError("invalid GRIB index offset/length")
    reset = any(b.offset < a.end for a, b in zip(records, records[1:]))
    if not reset:
        if any(r.end > object_size for r in records):
            raise ValueError("index extends beyond GRIB object")
        return records, "original"
    if sum(r.length for r in records) != object_size:
        raise ValueError("legacy index lengths do not cover the GRIB object")
    result, offset = [], 0
    for record in records:
        result.append(record.model_copy(update={"offset": offset}))
        offset += record.length
    return result, "cumulative_lengths_verified"
