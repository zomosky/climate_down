"""Bounded public IFS 0.1-degree transfer benchmark; never changes production.

Run from download/: uv run python docs/validation/ifs-0p1-speed-20261021/benchmark.py
Add --remote for 241/zhangmy-dev, bypassing proxy environment variables there.
Only complete GRIB messages selected through the real sidecar index are fetched.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime as dt
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import time

import requests

HOSTS = {
    "google": "https://storage.googleapis.com/ecmwf-open-data",
    "aws": "https://ecmwf-forecasts.s3.eu-central-1.amazonaws.com",
    "ecmwf": "https://data.ecmwf.int/forecasts",
}
SURFACE = (
    "100u", "100v", "10u", "10v", "10fg", "2t", "skt", "mn2t3", "mx2t3",
    "2d", "sp", "msl", "ssrd", "strd", "ssr", "str", "ttr", "tcc",
    "tp", "cp", "sf", "ro", "tcwv", "mucape",
)
PRESSURE = ("u", "v", "t", "q", "r", "gh")
MAX_MESSAGE_BYTES = 16 * 1024**2
MAX_BATCH_BYTES = 128 * 1024**2


def emit(value: dict) -> None:
    print(json.dumps(value, ensure_ascii=False), flush=True)


def url_for(host: str, shard: str, args, suffix: str) -> str:
    return (f"{HOSTS[host]}/{args.date}/{args.cycle:02d}z/ifs/0p1/oper/"
            f"{args.date}{args.cycle:02d}0000-{args.step}h-oper-fc-{shard}.{suffix}")


def session_for(args) -> requests.Session:
    session = requests.Session()
    session.trust_env = not args.direct
    session.headers["Accept-Encoding"] = "identity"
    return session


def index_for(host: str, shard: str, args) -> tuple[dict, list[dict]]:
    url = url_for(host, shard, args, "index")
    result = {"host": host, "shard": shard, "url": url}
    began = time.perf_counter()
    records = []
    try:
        with session_for(args) as session:
            response = session.get(url, timeout=(6, 10))
            result["status"] = response.status_code
            response.raise_for_status()
            if len(response.content) > 512 * 1024:
                raise ValueError("index exceeds benchmark cap")
            records = [json.loads(line) for line in response.text.splitlines() if line.strip()]
            result.update(bytes=len(response.content), records=len(records),
                          sha256=hashlib.sha256(response.content).hexdigest())
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    result["elapsed_s"] = round(time.perf_counter() - began, 4)
    return result, records


def transfer(host: str, shard: str, record: dict, args, deadline: float) -> dict:
    offset, length = int(record["_offset"]), int(record["_length"])
    url = url_for(host, shard, args, "grib2")
    result = {"host": host, "shard": shard, "param": record["param"],
              "levelist": record.get("levelist"), "url": url,
              "offset": offset, "expected_bytes": length, "bytes_read": 0}
    began = time.perf_counter()
    payload = bytearray()
    try:
        if not 0 < length <= MAX_MESSAGE_BYTES:
            raise ValueError("message exceeds benchmark cap")
        if began >= deadline:
            raise TimeoutError("batch deadline reached before request")
        with session_for(args) as session:
            with session.get(url, headers={"Range": f"bytes={offset}-{offset+length-1}"},
                             stream=True, timeout=(6, 10)) as response:
                result.update(status=response.status_code,
                              content_range=response.headers.get("Content-Range"),
                              headers_s=round(time.perf_counter() - began, 4))
                if response.status_code != 206:
                    raise ValueError(f"range request returned {response.status_code}; full-file transfer refused")
                expected = f"bytes {offset}-{offset+length-1}/"
                if not response.headers.get("Content-Range", "").startswith(expected):
                    raise ValueError("unexpected Content-Range")
                for chunk in response.iter_content(65536):
                    if time.perf_counter() >= deadline:
                        raise TimeoutError("benchmark batch deadline reached")
                    payload.extend(chunk)
                    if len(payload) > length:
                        raise ValueError("response exceeds requested range")
        if len(payload) != length or payload[:4] != b"GRIB" or payload[-4:] != b"7777":
            raise ValueError("incomplete GRIB message")
        if int.from_bytes(payload[8:16], "big") != length:
            raise ValueError("GRIB totalLength differs from index")
        result["complete"] = True
        result["sha256"] = hashlib.sha256(payload).hexdigest()
        if args.save_dir:
            folder = Path(args.save_dir)
            folder.mkdir(parents=True, exist_ok=True)
            path = folder / f"{host}-{shard}-{record['param']}-{record.get('levelist', 'sfc')}.grib2"
            path.write_bytes(payload)
            result["saved_path"] = str(path.resolve())
    except Exception as exc:
        result["complete"] = False
        result["error"] = f"{type(exc).__name__}: {exc}"
    result.update(bytes_read=len(payload), elapsed_s=round(time.perf_counter() - began, 4))
    result["MiB_per_s_including_connect"] = round(len(payload) / 1024**2 / max(result["elapsed_s"], 1e-6), 4)
    return result


def run(args) -> dict:
    report = {"tested_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
              "location": args.label, "direct": args.direct, "date": args.date,
              "cycle": args.cycle, "step": args.step, "indices": [],
              "single_field": [], "full_subset": None,
              "limits": {"workers": 4, "max_batch_bytes": MAX_BATCH_BYTES,
                         "max_batch_s": 90, "payload_includes_connect": True},
              "limitations": ["Short public-data sample; speed varies with routing and load.",
                              "No production configuration, service or scheduler changed.",
                              "GRIB framing and hashes checked; decode is a separate check."]}
    indices = {}
    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(index_for, host, shard, args): (host, shard)
                   for host in args.hosts.split(",") for shard in ("sfc", "pl")}
        for future in as_completed(futures):
            key = futures[future]
            status, records = future.result()
            report["indices"].append(status)
            indices[key] = records
            emit({"event": "index", **status})
    # Mirror samples run sequentially to avoid competing for the same bandwidth.
    for host in args.hosts.split(","):
        record = next((r for r in indices[(host, "sfc")] if r["param"] == "2t"), None)
        if record is None:
            continue
        result = transfer(host, "sfc", record, args, time.perf_counter() + 35)
        report["single_field"].append(result)
        emit({"event": "single_field", **result})
    good = [r for r in report["single_field"] if r["complete"] and indices[(r["host"], "pl")]]
    if args.full and good:
        best = min(good, key=lambda r: r["elapsed_s"])["host"]
        selected = []
        missing = []
        for shard, params in (("sfc", SURFACE), ("pl", PRESSURE)):
            for param in params:
                matches = [r for r in indices[(best, shard)] if r["param"] == param
                           and (shard == "sfc" or str(r.get("levelist")) == "850")]
                if not matches:
                    missing.append(f"{shard}:{param}")
                selected.extend((shard, record) for record in matches)
        total = sum(int(record["_length"]) for _, record in selected)
        batch = {"host": best, "workers": 4, "selected_messages": len(selected),
                 "expected_bytes": total, "missing_fields": missing, "transfers": []}
        report["full_subset"] = batch
        if total > MAX_BATCH_BYTES:
            batch["error"] = "full subset exceeds benchmark cap"
        else:
            began = time.perf_counter()
            emit({"event": "batch_start", **{k: v for k, v in batch.items() if k != "transfers"}})
            with ThreadPoolExecutor(max_workers=4) as pool:
                futures = [pool.submit(transfer, best, shard, record, args, began + 90)
                           for shard, record in selected]
                for future in as_completed(futures):
                    result = future.result()
                    batch["transfers"].append(result)
                    emit({"event": "batch_field", "param": result["param"],
                          "shard": result["shard"], "complete": result["complete"],
                          "bytes_read": result["bytes_read"], "elapsed_s": result["elapsed_s"],
                          "error": result.get("error")})
            batch["elapsed_s"] = round(time.perf_counter() - began, 4)
            batch["bytes_read"] = sum(r["bytes_read"] for r in batch["transfers"])
            batch["complete"] = bool(selected) and all(r["complete"] for r in batch["transfers"])
            batch["MiB_per_s_including_connect"] = round(batch["bytes_read"] / 1024**2 / max(batch["elapsed_s"], 1e-6), 4)
    report["finished_at_utc"] = dt.datetime.now(dt.timezone.utc).isoformat()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", action="store_true")
    parser.add_argument("--direct", action="store_true")
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--hosts", default="google,aws,ecmwf")
    parser.add_argument("--date", default="20261020")
    parser.add_argument("--cycle", type=int, default=12)
    parser.add_argument("--step", type=int, default=3)
    parser.add_argument("--label", default="local macOS / current proxy environment")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--save-dir")
    args = parser.parse_args()
    if args.remote:
        command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
                   "zhangmingyu@192.168.1.241", "docker", "exec", "-i", "zhangmy-dev",
                   "/workspace/climate_reanalysis/.venv/bin/python", "-", "--direct", "--full",
                   "--label", "241-zhangmy-dev-direct", "--date", args.date,
                   "--cycle", str(args.cycle), "--step", str(args.step), "--hosts", args.hosts]
        # No shell interpolation, credentials or remote files are involved.
        result = subprocess.run(command, input=Path(__file__).read_text(),
                                text=True, capture_output=True, timeout=190)
        print(result.stdout, end="", flush=True)
        if result.returncode:
            raise RuntimeError(result.stderr[-2000:])
        events = [json.loads(line) for line in result.stdout.splitlines() if line.strip()]
        report = next(event["report"] for event in reversed(events) if event.get("event") == "report")
    else:
        report = run(args)
        emit({"event": "report", "report": report})
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")


if __name__ == "__main__":
    main()
