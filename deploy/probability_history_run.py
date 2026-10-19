#!/usr/bin/env python3
"""Host runner: resumable oldest-first history; one init at a time, bounded resources.

Queue is an immutable GCS inventory snapshot. A state JSON per init is published
only after download AND eager Zarr verification; unavailable upstream events have
a distinct state. All progress files live in the data tree, readable by watcher.
"""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import time
from zoneinfo import ZoneInfo

SOURCES = {"ifs":"ifs-ens-probabilities", "aifs-ens":"aifs-ens-probabilities"}


def atomic(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n")
    os.replace(temporary, path)


def daily_priority(now):
    local = now.astimezone(ZoneInfo("Asia/Shanghai"))
    minutes = local.hour * 60 + local.minute
    return any(start <= minutes < end for start,end in ((300,390), (1020,1110)))


def pending_inits(queue, states):
    """Retry interrupted/failed inits before extending the history frontier."""
    def status(item):
        ident = f'{item["model"]}_{item["date"]}_{item["cycle"]:02}'
        return states.get(ident, {}).get("status", "pending")
    pending = [item for item in queue if status(item) not in ("done", "unavailable")]
    # Stable sorting preserves the queue's per-source chronological order.
    return sorted(pending, key=lambda item: status(item) != "failed")


def inside(directory, argv):
    # Bound the process INSIDE the container, so disconnecting docker exec
    # cannot leave an orphan still writing. Two phases fit the 15-minute
    # priority margin (2 * (360 + 15) = 750 seconds).
    return ["docker","exec","-w",directory,os.environ.get("CONTAINER","zhangmy-dev"),
            "timeout","--signal=TERM","--kill-after=15s","360s","env",
            "OMP_NUM_THREADS=1","OPENBLAS_NUM_THREADS=1","NUMEXPR_NUM_THREADS=1",
            "uv","run","--no-sync",*argv]


def run_one(item, log):
    model,date,cycle = item["model"],item["date"],item["cycle"]
    source = SOURCES[model]
    manifest = f"/climate_data/{source}/{date}/{cycle:02}z/{date}_{cycle:02}z_{source}.manifest.json"
    commands = [inside("/workspace/climate_down", ["python","scripts/backfill_probability_init.py",
        "--model",model,"--date",date,"--cycle",str(cycle),"--workers","6",
        "--layout",item.get("layout",f"{model}/0p25")]),
        inside("/workspace/climate_restorage", ["python","scripts/restore_probability_history.py","--manifest",manifest])]
    output = {}
    for phase, command in zip(("download","restore"), commands):
        # Per-request retries stay in the project; the enclosing timeout runs
        # in the container and bounds the complete producer process.
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=log, text=True, check=False)
        log.write(result.stdout); log.flush()
        if result.returncode:
            raise RuntimeError(f"{phase} exited {result.returncode}; see per-init log")
        lines = [s for s in result.stdout.splitlines() if s.strip()]
        output[phase] = json.loads(lines[-1])
    return output


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue",type=Path,required=True)
    parser.add_argument("--state-root",type=Path,default=Path("/training_data/climate_data/_probability_backfill"))
    parser.add_argument("--lock",type=Path,default=Path("/home/zhangmingyu/operation/lock/ens_probabilities.lock"))
    parser.add_argument("--max-passes",type=int,default=5)
    parser.add_argument("--limit",type=int,default=0)
    parser.add_argument("--ignore-priority",action="store_true",help="For a small supervised acceptance run only")
    args=parser.parse_args(argv)
    queue_raw=args.queue.read_bytes()
    queue=json.loads(queue_raw)["inits"]
    queue_sha=hashlib.sha256(queue_raw).hexdigest()
    if args.limit: queue=queue[:args.limit]
    root=args.state_root
    root.mkdir(parents=True,exist_ok=True)
    runner_lock=root/"runner.lock"
    with runner_lock.open("a") as singleton:
        try: fcntl.flock(singleton,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError: return 0
        if not args.limit and not args.ignore_priority:
            atomic(root/"queue.json",json.loads(queue_raw))
        prior_progress=root/"progress.json"
        if prior_progress.is_file() and not args.limit:
            previous=json.loads(prior_progress.read_text())
            if (previous.get("status")=="complete" and previous.get("queue_sha256")==queue_sha
                    and not previous.get("limited",False)):
                return 0
        states={}
        for path in (root/"inits").glob("*.json"):
            state=json.loads(path.read_text())
            if state.get("status") in ("done","unavailable") and "missing_samples" not in state.get("download",{}):
                source=SOURCES[state["model"]]
                manifest=root.parent/source/state["date"]/f'{state["cycle"]:02}z'/f'{state["date"]}_{state["cycle"]:02}z_{source}.manifest.json'
                if manifest.is_file():
                    contract=json.loads(manifest.read_text()).get("probability_availability",{})
                    state["download"].update({k:contract[k] for k in ("missing_samples","available_samples") if k in contract})
                    atomic(path,state)
            states[path.stem]=state
        def key(item): return f'{item["model"]}_{item["date"]}_{item["cycle"]:02}'
        def publish(active=None,status="running"):
            summary={}
            for model,source in SOURCES.items():
                targets=[item for item in queue if item["model"]==model]
                current=[states.get(key(item),{}) for item in targets]
                counts=Counter(state.get("status","pending") for state in current)
                gaps={}
                for item,state in zip(targets,current):
                    for variable,ends in state.get("download",{}).get("missing_samples",{}).items():
                        if not ends: continue
                        gap=gaps.setdefault(variable,dict(variable=variable,missing_inits=0,missing_windows=0,
                            first_init=None,last_init=None,examples=[]))
                        label=f'{item["date"]}_{item["cycle"]:02}z'
                        gap["missing_inits"]+=1;gap["missing_windows"]+=len(ends)
                        gap["first_init"]=min(gap["first_init"] or label,label)
                        gap["last_init"]=max(gap["last_init"] or label,label)
                        if len(gap["examples"])<3: gap["examples"].append(dict(date=item["date"],cycle=item["cycle"]))
                summary[source]=dict(total=len(targets),counts=dict(counts),
                    grids=dict(Counter(str(i.get("grid_resolution_degrees",0.25)) for i in targets)),
                    earliest_init=min((f'{i["date"]}_{i["cycle"]:02}z' for i in targets),default=None),
                    latest_init=max((f'{i["date"]}_{i["cycle"]:02}z' for i in targets),default=None),
                    bytes=sum(s.get("download",{}).get("bytes",0) for s in current),
                    partial=sum(s.get("download",{}).get("status")=="partial" for s in current),
                    gaps=sorted(gaps.values(),key=lambda g:g["variable"]))
            atomic(root/"progress.json",dict(updated_at=datetime.now(timezone.utc).isoformat(),status=status,
                active=active,sources=summary,queue_file=str(args.queue),queue_sha256=queue_sha,limited=bool(args.limit),
                order="failed_first_then_oldest_per_source"))
        publish()
        for attempt in range(args.max_passes):
            pending=pending_inits(queue, states)
            if not pending:
                publish(status="complete");return 0
            for item in pending:
                ident=key(item)
                while not args.ignore_priority and daily_priority(datetime.now(timezone.utc)):
                    publish(status="daily_priority");time.sleep(60)
                if shutil.disk_usage(root).free < 100*1024**3:
                    publish(status="paused_low_disk");return 2
                args.lock.parent.mkdir(parents=True,exist_ok=True)
                with args.lock.open("a") as shared:
                    # Let ongoing daily runs or cold moves finish; release after
                    # EACH init so the historical job never monopolises the lock.
                    fcntl.flock(shared,fcntl.LOCK_EX)
                    if not args.ignore_priority and daily_priority(datetime.now(timezone.utc)):
                        continue
                    started=datetime.now(timezone.utc).isoformat()
                    publish(active=dict(**item,started_at=started))
                    logpath=root/"logs"/f"{ident}.log"
                    logpath.parent.mkdir(parents=True,exist_ok=True)
                    with logpath.open("a") as log:
                        init_attempt = states.get(ident, {}).get("attempt", 0) + 1
                        try:
                            result=run_one(item,log)
                            state=dict(**item,**result,status="unavailable" if result["download"]["status"]=="unavailable" else "done",
                                       attempt=init_attempt,started_at=started,finished_at=datetime.now(timezone.utc).isoformat())
                        except Exception as exc:
                            state=dict(**item,status="failed",attempt=init_attempt,error=str(exc),started_at=started,
                                       finished_at=datetime.now(timezone.utc).isoformat())
                        states[ident]=state
                        atomic(root/"inits"/f"{ident}.json",state)
                        publish()
                        print(json.dumps(state,ensure_ascii=False),flush=True)
                time.sleep(1)
            if attempt+1 < args.max_passes:
                time.sleep(60)
        remaining=[item for item in queue if states.get(key(item),{}).get("status") not in ("done","unavailable")]
        publish(status="failed" if remaining else "complete")
        return 1 if remaining else 0


if __name__=="__main__":
    raise SystemExit(main())
