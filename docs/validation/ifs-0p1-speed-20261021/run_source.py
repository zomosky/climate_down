"""One-init / one-lead real download acceptance for the independent source.

Run from download/: uv run python docs/validation/ifs-0p1-speed-20261021/run_source.py
Outputs are isolated under output/ifs-0p1-speed-20261021/pipeline/.
"""
from __future__ import annotations

from collections import Counter
import hashlib
import json
from pathlib import Path
import time

import eccodes
import numpy as np

from climate_download.config import load_job
from climate_download.jobs import run_job
from climate_download.sources._http import build_client


def main() -> None:
    directory = Path(__file__).resolve().parent
    cfg = load_job("config/jobs/ifs_0p1_renewables.yaml")
    cfg.time.date, cfg.time.cycle, cfg.time.steps = "20261020", 12, [3]
    cfg.download.output_dir = Path("output/ifs-0p1-speed-20261021/pipeline")
    cfg.download.max_attempts = 2
    cfg.download.timeout_seconds = 30
    with build_client(15) as client:
        steps = cfg.source.list_available_steps(client, date="20261020", cycle=12)
    began = time.perf_counter()
    outcome = run_job(cfg)
    elapsed = time.perf_counter() - began
    if outcome.failed or len(outcome.succeeded) != 1:
        raise RuntimeError(f"source run did not pass: {outcome.failed}")
    result = outcome.succeeded[0]
    if result.resumed:
        raise RuntimeError("refusing to count a resumed file as a speed measurement")
    reference = json.loads((directory / "241-results.json").read_text())["full_subset"]
    expected = Counter(item["sha256"] for item in reference["transfers"])
    decoded, actual = [], Counter()
    with result.output_path.open("rb") as handle:
        while (message := eccodes.codes_grib_new_from_file(handle)) is not None:
            try:
                keys = ["shortName", "paramId", "typeOfLevel", "level", "gridType", "Ni", "Nj",
                        "iDirectionIncrementInDegrees", "jDirectionIncrementInDegrees",
                        "packingType", "bitsPerValue", "stepType", "startStep", "endStep", "units"]
                row = {key: eccodes.codes_get(message, key) for key in keys}
                values = eccodes.codes_get_values(message)
                row.update(values=int(values.size), finite_count=int(np.isfinite(values).sum()),
                           missing_count=int(eccodes.codes_get(message, "numberOfMissing")),
                           sha256=hashlib.sha256(eccodes.codes_get_message(message)).hexdigest())
                if (row["Ni"], row["Nj"]) != (3600, 1801):
                    raise ValueError("unexpected 0.1-degree grid dimensions")
                if row["iDirectionIncrementInDegrees"] != 0.1 or row["jDirectionIncrementInDegrees"] != 0.1:
                    raise ValueError("unexpected 0.1-degree grid spacing")
                if row["endStep"] != 3 or row["values"] != 6483600:
                    raise ValueError("unexpected lead or data-point count")
                decoded.append(row)
                actual[row["sha256"]] += 1
            finally:
                eccodes.codes_release(message)
    if actual != expected or len(decoded) != 30:
        raise ValueError("merged source messages differ from independent 241 sample")
    report = {
        "date": "20261020", "cycle": 12, "step": 3, "source": cfg.source.name,
        "listed_steps": steps, "variables": len(decoded), "fully_decoded": True,
        "all_messages_match_independent_241_samples": True,
        "elapsed_s_including_metadata_manifest": round(elapsed, 4),
        "bytes": result.bytes_downloaded, "MiB": round(result.bytes_downloaded / 1024**2, 4),
        "MiB_per_s_including_metadata_manifest": round(result.bytes_downloaded / 1024**2 / elapsed, 4),
        "http_payload_requests": result.http_requests, "resumed": result.resumed,
        "output_path": str(result.output_path.resolve()),
        "sha256": hashlib.sha256(result.output_path.read_bytes()).hexdigest(),
        "run_report": str(outcome.report_path.resolve()), "fields": decoded,
    }
    (directory / "source-acceptance.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    print(json.dumps({key: value for key, value in report.items() if key != "fields"}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
