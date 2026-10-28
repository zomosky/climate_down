"""Strict native coverage and byte-preserving per-step publication.

The standard manifest is the only ready signal. Original ADS groups, individual
message hashes and original statistical windows remain in .cams receipts.
"""
from __future__ import annotations

from collections import Counter
import datetime as dt
import hashlib
from pathlib import Path
from typing import Iterable

from climate_download.cams.catalogue import ENERGY, PRECIP, SOURCE, VARIABLES, Variable
from climate_download.cams.state import digest, now, save, signature

HEADER_KEYS = (
    "dataDate", "dataTime", "paramId", "shortName", "dataType", "typeOfLevel", "level",
    "units", "stepType", "startStep", "endStep", "stepUnits", "validityDate", "validityTime",
    "Ni", "Nj", "latitudeOfFirstGridPointInDegrees", "longitudeOfFirstGridPointInDegrees",
    "latitudeOfLastGridPointInDegrees", "longitudeOfLastGridPointInDegrees",
    "iDirectionIncrementInDegrees", "jDirectionIncrementInDegrees", "numberOfMissing",
)
GRID = {"Ni": 176, "Nj": 100, "latitudeOfFirstGridPointInDegrees": 54.8,
        "longitudeOfFirstGridPointInDegrees": 70.0, "latitudeOfLastGridPointInDegrees": 15.2,
        "longitudeOfLastGridPointInDegrees": 140.0,
        "iDirectionIncrementInDegrees": 0.4, "jDirectionIncrementInDegrees": 0.4}


def iter_messages(path: Path):
    import eccodes as ec
    import numpy as np

    with path.open("rb") as stream:
        while (handle := ec.codes_grib_new_from_file(stream)) is not None:
            try:
                header = {key: ec.codes_get(handle, key) for key in HEADER_KEYS}
                param, lead = int(header["paramId"]), int(header["endStep"])
                if param in (169, 210207, 165, 166, 167) or (lead == 0 and param in ENERGY | PRECIP):
                    values = ec.codes_get_values(handle)
                    if header["numberOfMissing"] or not np.isfinite(values).all():
                        raise ValueError("Missing/non-finite core or accumulated h0 values")
                    if lead == 0 and param in ENERGY | PRECIP and not np.all(values == 0):
                        raise ValueError("Nonzero accumulated h0")
                yield header, ec.codes_get_message(handle)
            finally:
                ec.codes_release(handle)


def check_header(header: dict, date: str, cycle: int, variables: dict[int, Variable]) -> tuple[int, int]:
    p, h = int(header["paramId"]), int(header["endStep"])
    if p not in variables or h not in variables[p].leads:
        raise ValueError(f"Unexpected CAMS parameter/native lead {(p, h)}")
    v = variables[p]
    if (header["dataDate"], header["dataTime"], header["dataType"]) != (int(date), cycle * 100, "fc"):
        raise ValueError("Foreign init or non-forecast data")
    if (header["typeOfLevel"], header["level"], header["units"], header["shortName"]) != (
            v.type_of_level, v.level, v.units, v.short_name):
        raise ValueError(f"Changed level, units or shortName for {p}")
    if int(header["stepUnits"]) != 1:
        raise ValueError("CAMS lead unit must be hours")
    for key, value in GRID.items():
        if abs(float(header[key]) - value) > 1e-6:
            raise ValueError(f"Changed CAMS grid: {key}")
    valid = dt.datetime.strptime(date, "%Y%m%d").replace(hour=cycle) + dt.timedelta(hours=h)
    if (header["validityDate"], header["validityTime"]) != (
            int(valid.strftime("%Y%m%d")), int(valid.strftime("%H%M"))):
        raise ValueError("Shifted native UTC label")
    kind, start = header["stepType"], int(header["startStep"])
    if p in ENERGY | PRECIP:
        # ADS currently labels some cumulative fields instant. Preserve that
        # raw encoding, normalize semantics in the CAMS restore adapter only.
        if kind not in ("instant", "accum") or start != (h if kind == "instant" else 0):
            raise ValueError("Changed cumulative forecast window encoding")
    elif p == 228028:
        if (kind, start) != (("instant", 0) if h == 0 else ("max", h - 3)):
            raise ValueError("Changed three-hour gust window")
    elif (kind, start) != ("instant", h):
        raise ValueError("Changed instantaneous field window")
    return p, h


def validate(paths: Iterable[Path], date: str, cycle: int,
             variables: tuple[Variable, ...] = VARIABLES, *, split_dir: Path | None = None,
             native_leads: tuple[int, ...] | None = None) -> dict:
    by_param = {v.param_id: v for v in variables}
    if native_leads is not None and (not native_leads or len(native_leads) != len(set(native_leads))
            or any(not set(native_leads) <= set(v.leads) for v in variables)):
        raise ValueError('Invalid CAMS native subset leads')
    expected = {(v.param_id, h) for v in variables for h in (v.leads if native_leads is None else native_leads)}
    seen, records, originals, handles, split_hashes = set(), [], [], {}, {}
    counts: Counter = Counter()
    try:
        for path in paths:
            size = 0
            raw_messages_hash = hashlib.sha256()
            for header, message in iter_messages(path):
                p, h = check_header(header, date, cycle, by_param)
                if (p, h) not in expected:
                    raise ValueError(f'Unexpected CAMS piece parameter/native lead {(p, h)}')
                if (p, h) in seen:
                    raise ValueError(f"Duplicate CAMS parameter/native lead {(p, h)}")
                seen.add((p, h))
                size += len(message)
                raw_messages_hash.update(message)
                counts[h] += 1
                records.append({"param_id": p, "lead_hours": h, "bytes": len(message),
                                "sha256": hashlib.sha256(message).hexdigest(),
                                "source_step_type": header["stepType"], "start_step": header["startStep"],
                                "source_missing_values": header["numberOfMissing"]})
                if split_dir is not None:
                    if h not in handles:
                        handles[h] = (split_dir / f"f{h:03d}.subset.grib2.part").open("wb")
                        split_hashes[h] = hashlib.sha256()
                    handles[h].write(message)
                    split_hashes[h].update(message)
            if size != path.stat().st_size:
                raise ValueError("Unaccounted raw bytes (truncated/trailing/foreign data)")
            raw_sha = digest(path)
            if raw_messages_hash.hexdigest() != raw_sha:
                raise ValueError("Decoded message bytes differ from the original raw GRIB bytes")
            originals.append({"path": str(path), "size_bytes": size, "sha256": raw_sha})
        if seen != expected:
            raise ValueError(f"Missing {len(expected - seen)} native identities: {sorted(expected - seen)[:5]}")
    finally:
        for stream in handles.values():
            stream.close()
    for h, expected_hash in split_hashes.items():
        if digest(split_dir / f"f{h:03d}.subset.grib2.part") != expected_hash.hexdigest():
            raise ValueError("Split GRIB bytes differ from the original-message stream")
    return {"passed": True, "date": date, "cycle": cycle, "messages": len(seen),
            "all_original_bytes_accounted_for": True, "originals": originals,
            "records": records, "messages_per_step": dict(sorted(counts.items())),
            "split_sha256": {str(h): s.hexdigest() for h, s in sorted(split_hashes.items())},
            "native_samples": {str(v.param_id): len(v.leads if native_leads is None else native_leads)
                               for v in variables}}


def manifest_path(folder: Path, date: str, cycle: int) -> Path:
    return folder / f"{date}_{cycle:02d}z_{SOURCE}.manifest.json"


def verify_published(folder: Path, date: str, cycle: int) -> bool:
    import json
    path = manifest_path(folder, date, cycle)
    if not path.exists():
        return False
    payload = json.loads(path.read_text())
    if (payload.get("cams_contract_sha256") != signature([v.__dict__ for v in VARIABLES])
            or payload.get("date") != date or payload.get("cycle") != cycle
            or payload.get("source", {}).get("name") != SOURCE
            or not payload.get("completed_at") or payload.get("failures")
            or [f["step_hours"] for f in payload["files"]] != list(range(121))):
        raise ValueError("Existing CAMS manifest does not satisfy the fixed complete contract")
    for item in payload["files"]:
        file = folder / Path(item["path"]).name
        if not file.is_file() or file.stat().st_size != item["size_bytes"] or digest(file) != item["sha256"]:
            raise ValueError("Published CAMS GRIB no longer matches its manifest")
    return True


def publish(paths: list[Path], folder: Path, date: str, cycle: int, requests: list[dict]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    if verify_published(folder, date, cycle):
        return manifest_path(folder, date, cycle)
    receipt = validate(paths, date, cycle, split_dir=folder)
    save(folder / ".cams" / "raw_validation.json", receipt)
    files = []
    for h in range(121):
        path = folder / f"f{h:03d}.subset.grib2"
        path.with_suffix(path.suffix + ".part").replace(path)
        files.append({"step_hours": h, "path": str(path.resolve()), "size_bytes": path.stat().st_size,
                      "sha256": digest(path), "records_selected": receipt["messages_per_step"][h]})
    init = dt.datetime.strptime(date, "%Y%m%d").replace(hour=cycle, tzinfo=dt.UTC)
    payload = {"schema_version": 1, "source": {"name": SOURCE, "description": "CAMS global ADS 0.4° PV/wind"},
               "date": date, "cycle": cycle, "init_time": init.isoformat(), "completed_at": now(),
               "failures": [], "files": files,
               "variables": [{"name": v.api_name, "levtype": "pl" if v.pressure_level else "sfc",
                              "params": [v.param_id], "levels": [850] if v.pressure_level else None,
                              "native_step_hours": list(v.leads)} for v in VARIABLES],
               "cams_contract_sha256": signature([v.__dict__ for v in VARIABLES]),
               "expected_messages": 5732, "original_requests": requests,
               "download": {"workers": 1, "protocol": "ADS submit-poll-resume", "area": [55, 70, 15, 140]},
               "raw_validation": ".cams/raw_validation.json"}
    path = manifest_path(folder, date, cycle)
    save(path, payload)
    return path
