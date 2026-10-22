"""Run the new adapter in memory on 241, with temporary outputs only.

The production package, configs, services and scheduler are not edited.
"""
from __future__ import annotations

import json
from pathlib import Path
import subprocess


REMOTE = r'''
import hashlib
import json
import os
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import time
import types

payload = __PAYLOAD__
for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(name, None)
from climate_download.config import load_job
from climate_download.jobs import run_job
from climate_download.sources.registry import SOURCE_REGISTRY
if "ifs-split" in SOURCE_REGISTRY:
    raise RuntimeError("ifs-split already exists remotely; refusing to replace it")
module = types.ModuleType("climate_download.sources.ifs_split")
sys.modules[module.__name__] = module
exec(compile(payload["module"], "ifs_split.py", "exec"), module.__dict__)
with TemporaryDirectory(prefix="ifs-0p1-speed-") as temporary:
    root = Path(temporary)
    for name, content in payload["configs"].items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    cfg = load_job(root / "config/jobs/ifs_0p1_renewables.yaml")
    cfg.time.date, cfg.time.cycle, cfg.time.steps = "20261020", 12, [3]
    cfg.download.output_dir = root / "output"
    cfg.download.max_attempts = 2
    cfg.download.timeout_seconds = 30
    began = time.perf_counter()
    outcome = run_job(cfg)
    elapsed = time.perf_counter() - began
    if outcome.failed or len(outcome.succeeded) != 1:
        raise RuntimeError(f"source run failed: {outcome.failed}")
    result = outcome.succeeded[0]
    digest = hashlib.sha256(result.output_path.read_bytes()).hexdigest()
    if result.resumed or digest != payload["expected_sha256"] or result.records_selected != 30:
        raise RuntimeError("remote product does not match the fully decoded local product")
    manifest_path = next(result.output_path.parent.glob("*.manifest.json"))
    report = {"location": "241 / zhangmy-dev / direct", "source": cfg.source.name,
              "date": "20261020", "cycle": 12, "step": 3,
              "variables": result.records_selected, "bytes": result.bytes_downloaded,
              "MiB": round(result.bytes_downloaded / 1024**2, 4),
              "elapsed_s_including_metadata_manifest": round(elapsed, 4),
              "MiB_per_s_including_metadata_manifest": round(result.bytes_downloaded / 1024**2 / elapsed, 4),
              "workers": cfg.download.workers, "http_payload_requests": result.http_requests,
              "sha256": digest, "matches_fully_decoded_local_product": True,
              "temporary_outputs_cleaned_after_run": True,
              "production_files_changed": False,
              "manifest": json.loads(manifest_path.read_text())}
print("IFS_0P1_RESULT=" + json.dumps(report))
'''


def main() -> None:
    root = Path(__file__).resolve().parents[3]
    output = Path(__file__).with_name("241-source-acceptance.json")
    reference = json.loads(Path(__file__).with_name("source-acceptance.json").read_text())
    paths = ["config/sources/ifs_0p1.yaml", "config/jobs/ifs_0p1_renewables.yaml"]
    payload = {"module": (root / "src/climate_download/sources/ifs_split.py").read_text(),
               "configs": {path: (root / path).read_text() for path in paths},
               "expected_sha256": reference["sha256"]}
    script = REMOTE.replace("__PAYLOAD__", repr(payload))
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8",
               "zhangmingyu@192.168.1.241", "docker", "exec", "-i", "zhangmy-dev",
               "/workspace/climate_down/.venv/bin/python", "-"]
    result = subprocess.run(command, input=script, text=True, capture_output=True, timeout=160)
    Path(__file__).with_name("241-source-run.txt").write_text(result.stdout + result.stderr)
    if result.returncode:
        raise RuntimeError(result.stderr[-3000:])
    line = next(line for line in result.stdout.splitlines() if line.startswith("IFS_0P1_RESULT="))
    report = json.loads(line.partition("=")[2])
    output.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "manifest"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
