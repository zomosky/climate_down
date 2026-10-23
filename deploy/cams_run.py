#!/usr/bin/env python3
"""Host cron entry: recent CAMS cycles, durable same-request recovery, no annual expansion."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import re
import subprocess
import time


def stamp():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def write(path, value):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    tmp.replace(path)


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--cycle", type=int, choices=(0, 12), required=True)
    p.add_argument("--once", action="store_true")
    p.add_argument("--preflight-only", action="store_true")
    p.add_argument("--trigger", choices=("cron", "manual"), default="manual")
    args = p.parse_args(argv)
    operation = Path(os.environ.get("CAMS_OPERATION_DIR", str(Path(__file__).resolve().parent / "cams")))
    operation.mkdir(parents=True, exist_ok=True)
    with (operation / f"worker-{args.cycle:02d}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        state = {"started_at": stamp(), "pid": os.getpid(), "parent_pid": os.getppid(),
                 "cycle": args.cycle, "trigger": args.trigger, "phase": "running"}
        state_path = operation / f"run-{args.cycle:02d}.json"
        write(state_path, state)
        command = ["docker", "exec", "-w", "/workspace/climate_down", "zhangmy-dev",
                   "/workspace/climate_down/.venv/bin/python", "-m", "climate_download.cli", "cams",
                   "--config", "config/jobs/cams_operational.yaml", "--cycle", str(args.cycle), "--scheduled"]
        if args.once:
            command.append("--once")
        if args.preflight_only:
            command.append("--preflight-only")
        started = time.monotonic()
        result = subprocess.run(command, capture_output=True, text=True)
        # Downloader already redacts exceptions. A second guard protects this
        # durable host log from accidental transport/library chatter.
        output = result.stdout + result.stderr
        output = re.sub(r"https?://[^\s\"'<>]+", "[URL_REDACTED]", output)
        output = re.sub(r"[\w.+-]+@[\w.-]+", "[EMAIL_REDACTED]", output)
        state.update(phase="finished" if result.returncode == 0 else "failed", finished_at=stamp(),
                     elapsed_seconds=round(time.monotonic() - started, 3), returncode=result.returncode,
                     summary=output[-5000:])
        write(state_path, state)
        with (operation / "runs.jsonl").open("a") as log:
            log.write(json.dumps(state, ensure_ascii=False) + "\n")
        print(json.dumps(state, ensure_ascii=False), flush=True)
        return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
