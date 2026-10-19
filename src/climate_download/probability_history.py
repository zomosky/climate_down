"""Historical probability acquisition with an explicit upstream-availability receipt.

Operational source strictness is deliberately unchanged. Empty historical bundles
are discovery results, whereas transport, offset, checksum and decode errors fail.
"""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import struct

from climate_download.grib.index import IndexRecord, merge_ranges, parse_index_text
from climate_download.grib.partial import PartialDownloader
from climate_download.sources._http import build_client, request_with_retry
from climate_download.sources.ecmwf_probabilities import expected_windows

ROOT = "https://storage.googleapis.com/ecmwf-open-data"
SOURCES = {"ifs": "ifs-ens-probabilities", "aifs-ens": "aifs-ens-probabilities"}


def atomic_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")
    os.replace(tmp, path)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def normalize_offsets(records: list[IndexRecord], object_size: int) -> tuple[list[IndexRecord], str]:
    """Repair concatenated em/ep/es indexes only if all lengths cover the object.

    Normal global offsets may legitimately have gaps (e.g. an index that only
    lists ep); do not silently rewrite those. A backwards/overlapping offset is
    the legacy signal. Never guess offsets if the total does not match.
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


def event_field(param: str, window: str) -> tuple[str, int]:
    pieces = window.split("-")
    start, end = (int(pieces[0]), int(pieces[1])) if len(pieces) == 2 else (int(window), int(window))
    width = end - start
    if width == 0 or width == 24 and (param.startswith("tpg") or param.startswith("10fgg")):
        return param, end
    stat = "max" if param == "2tl273" else "accum" if param == "tpl01" else "avg"
    return f"{param}_{stat}{width}h", end


def sample_catalog(model: str, bundles: list[dict] | None = None) -> dict[str, list[int]]:
    fields: dict[str, set[int]] = {}
    events = ((p, w) for bundle in bundles for p, w in bundle["available_events"]) if bundles is not None else (
        (p, w) for b in (240, 360) for p, windows in expected_windows(model, b).items() for w in windows)
    for param, window in events:
        field, end = event_field(param, window)
        fields.setdefault(field, set()).add(end)
    return {field: sorted(ends) for field, ends in sorted(fields.items())}


def availability(model: str, bundles: list[dict]) -> dict:
    expected, available = sample_catalog(model), sample_catalog(model, bundles)
    missing = {v: sorted(set(ends) - set(available.get(v, []))) for v, ends in expected.items()}
    missing = {v: ends for v, ends in missing.items() if ends}
    return dict(schema_version=1, policy="historical_available", model=model,
                status="unavailable" if not available else "partial" if missing else "complete",
                expected_samples=expected, available_samples=available, missing_samples=missing,
                missing_variables=sorted(set(expected) - set(available)), bundles=bundles,
                reason="upstream_archive_does_not_publish_these_events" if missing else "")


def validate_subset(path: Path, records: list[IndexRecord]) -> None:
    """Match every downloaded message boundary against the exact index selection."""
    with path.open("rb") as handle:
        for record in sorted(records, key=lambda r: r.offset):
            header = handle.read(16)
            if (len(header) != 16 or header[:4] != b"GRIB" or header[7] != 2
                or struct.unpack(">Q", header[8:])[0] != record.length):
                raise ValueError(f"GRIB message length/header mismatch: {path}")
            handle.seek(record.length - 20, 1)
            if handle.read(4) != b"7777":
                raise ValueError(f"GRIB message trailer mismatch: {path}")
        if handle.read(1):
            raise ValueError(f"unexpected extra GRIB bytes: {path}")


def download_init(*, model: str, date: str, cycle: int, raw_root: Path, workers: int = 4,
                  layout: str | None = None) -> dict:
    source = SOURCES[model]
    datetime.strptime(date, "%Y%m%d")
    if cycle not in ((0, 12) if model == "ifs" else (0, 6, 12, 18)):
        raise ValueError("invalid model cycle")
    layout = layout or f"{model}/0p25"
    if layout not in ({"ifs/0p25", "0p25", "0p4-beta"} if model == "ifs" else {"aifs-ens/0p25"}):
        raise ValueError("invalid historical model/grid layout")
    directory = raw_root / source / date / f"{cycle:02}z"
    directory.mkdir(parents=True, exist_ok=True)
    files, bundles = [], []
    with build_client(timeout=120) as client:
        for bundle in (240, 360):
            stem = f"{ROOT}/{date}/{cycle:02}z/{layout}/enfo/{date}{cycle:02}0000-{bundle}h-enfo-ep"
            index_response = request_with_retry(client, "GET", stem + ".index")
            data_response = request_with_retry(client, "HEAD", stem + ".grib2")
            receipt = dict(bundle=bundle, index_url=stem + ".index", data_url=stem + ".grib2", available_events=[])
            # A definite 404 is an upstream absence, never a network exception.
            if index_response.status_code == 404 or data_response.status_code == 404:
                for response in (index_response, data_response):
                    if response.status_code != 404:
                        response.raise_for_status()
                receipt.update(status="missing_bundle", index_status=index_response.status_code,
                               data_status=data_response.status_code)
                bundles.append(receipt)
                continue
            index_response.raise_for_status()
            data_response.raise_for_status()
            size = int(data_response.headers["Content-Length"])
            records, mode = normalize_offsets(parse_index_text(index_response.text), size)
            wanted = {(p, w) for p, windows in expected_windows(model, bundle).items() for w in windows}
            selected = [r for r in records if (r.model_extra or {}).get("type") == "ep" and (r.param, r.step) in wanted]
            events = [(r.param, r.step) for r in selected]
            if len(set(events)) != len(events):
                raise ValueError(f"duplicate probability events in bundle {bundle}")
            indexed_bytes = sum(r.length for r in records)
            if len(selected) != len(wanted) and indexed_bytes != size:
                raise ValueError("historical index does not cover the object; cannot declare upstream absence")
            index_path = directory / f"f{bundle}.upstream.index"
            index_path.write_text(index_response.text)
            signature = hashlib.sha256(json.dumps(dict(data_url=stem + ".grib2", object_size=size,
                object_etag=data_response.headers.get("ETag"), index_sha256=sha256(index_path),
                selected=[(r.param, r.step, r.offset, r.length) for r in selected]), sort_keys=True).encode()).hexdigest()
            receipt.update(status="available" if selected else "no_probability_fields", object_size=size,
                           indexed_bytes=indexed_bytes, index_covers_object=indexed_bytes == size,
                           object_etag=data_response.headers.get("ETag"),
                           index_sha256=sha256(index_path), offset_mode=mode,
                           selection_sha256=signature, available_events=sorted(events), selected_records=len(selected))
            bundles.append(receipt)
            if not selected:
                continue
            path = directory / f"f{bundle}.subset.grib2"
            proof = directory / f"f{bundle}.download.json"
            prior = json.loads(proof.read_text()) if proof.is_file() else {}
            resumed = path.is_file() and prior.get("selection_sha256") == signature and prior.get("sha256") == sha256(path)
            if not resumed:
                temporary = path.with_name(path.name + ".part")
                with PartialDownloader(client, max_workers=workers, max_attempts=4) as downloader:
                    downloader.download(stem + ".grib2", merge_ranges(selected), temporary)
                validate_subset(temporary, selected)
                os.replace(temporary, path)
            else:
                validate_subset(path, selected)
            checksum = sha256(path)
            atomic_json(proof, dict(selection_sha256=signature, sha256=checksum))
            files.append(dict(step_hours=bundle, path=str(path), size_bytes=path.stat().st_size,
                              sha256=checksum, records_selected=len(selected), records_total=len(records),
                              selected_breakdown=dict(Counter(r.param for r in selected))))
    contract = availability(model, bundles)
    contract.update(source_layout=layout, grid_resolution_degrees=0.4 if layout == "0p4-beta" else 0.25)
    manifest = dict(schema_version=1, source=dict(name=source, description="ECMWF ENS 历史事件概率"),
                    date=date, cycle=cycle,
                    init_time=datetime.strptime(f"{date}{cycle:02}", "%Y%m%d%H").replace(tzinfo=timezone.utc).isoformat(),
                    completed_at=datetime.now(timezone.utc).isoformat(), variables=[dict(name="event_probabilities", levtype="sfc",
                    params=sorted({p for b in bundles for p, w in b["available_events"]}), levels=None)],
                    files=files, failures=[], probability_availability=contract)
    target = directory / f"{date}_{cycle:02}z_{source}.manifest.json"
    # Keep the ready signal unchanged for an identical resumed acquisition.
    old = json.loads(target.read_text()) if target.is_file() else {}
    if {k: v for k, v in old.items() if k != "completed_at"} != {k: v for k, v in manifest.items() if k != "completed_at"}:
        atomic_json(target, manifest)
    return dict(source=source, date=date, cycle=cycle, manifest=str(target),
                status=contract["status"], variables=len(contract["available_samples"]),
                samples=sum(map(len, contract["available_samples"].values())),
                missing_variables=contract["missing_variables"], missing_samples=contract["missing_samples"],
                available_samples=contract["available_samples"], bytes=sum(f["size_bytes"] for f in files))
