"""Recovery policy for the host's single-writer historical backfill runner."""
import importlib.util
import json
from pathlib import Path


path = Path(__file__).resolve().parents[1] / "deploy/probability_history_run.py"
spec = importlib.util.spec_from_file_location("probability_history_runner", path)
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)


def item(date, cycle=0):
    return dict(model="aifs-ens", date=date, cycle=cycle)


def test_resume_repairs_failures_before_new_inits_without_reordering_each_group():
    queue = [item(f"202610{day:02}") for day in range(1, 7)]
    states = {
        "aifs-ens_20261001_00": {"status": "done"},
        "aifs-ens_20261003_00": {"status": "failed"},
        "aifs-ens_20261004_00": {"status": "unavailable"},
        "aifs-ens_20261006_00": {"status": "failed"},
    }
    assert runner.pending_inits(queue, states) == [queue[2], queue[5], queue[1], queue[4]]
    assert len(queue) == 6


def test_fresh_queue_order_and_completed_receipts_are_preserved():
    queue = [item("20261001", 12), item("20261001", 18), item("20261002")]
    assert runner.pending_inits(queue, {}) == queue
    states = {f'{i["model"]}_{i["date"]}_{i["cycle"]:02}': {"status": "done"} for i in queue}
    assert runner.pending_inits(queue, states) == []


def test_restarted_runner_preserves_receipts_and_counts_attempts(tmp_path, monkeypatch):
    queue = [item("20261001"), item("20261002"), item("20261003")]
    queue_path = tmp_path / "queue.json"
    queue_path.write_text(json.dumps({"inits": queue}))
    root = tmp_path / "state"
    (root / "inits").mkdir(parents=True)
    done = dict(**queue[0], status="done", attempt=2, download={"missing_samples": {}})
    done_path = root / "inits/aifs-ens_20261001_00.json"
    done_path.write_text(json.dumps(done))
    failed_path = root / "inits/aifs-ens_20261003_00.json"
    failed_path.write_text(json.dumps(dict(**queue[2], status="failed", attempt=4)))
    called = []

    def run_one(init, log):
        called.append(init["date"])
        return dict(download={"status": "complete"}, restore={"status": "done"})

    monkeypatch.setattr(runner, "run_one", run_one)
    monkeypatch.setattr(runner.time, "sleep", lambda _: None)
    monkeypatch.setattr(runner.shutil, "disk_usage", lambda _: type("Space", (), {"free": 200 * 1024**3})())
    assert runner.main(["--queue", str(queue_path), "--state-root", str(root),
                        "--lock", str(tmp_path / "writer.lock"), "--ignore-priority"]) == 0
    assert called == ["20261003", "20261002"]
    assert json.loads(done_path.read_text()) == done
    assert json.loads(failed_path.read_text())["attempt"] == 5
    assert json.loads((root / "progress.json").read_text())["status"] == "complete"
