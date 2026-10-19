import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location("probability_schedule", Path(__file__).parents[1] / "deploy/probabilities_run.py")
schedule = importlib.util.module_from_spec(spec)
spec.loader.exec_module(schedule)


def test_china_schedule_selects_correct_utc_init():
    assert schedule.init_dates(datetime(2026,10,15,21,15,tzinfo=timezone.utc),12) == ["20261015","20261014"]
    assert schedule.init_dates(datetime(2026,10,16,9,15,tzinfo=timezone.utc),0) == ["20261016","20261015"]


def test_partial_download_blocks_restore_and_is_not_success():
    calls=[]
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=1)
    assert not schedule.run_target("ifs-ens-probabilities","20261015",12,run)
    assert len(calls)==1


def test_success_restores_source_with_one_worker():
    calls=[]
    def run(argv, **kwargs):
        calls.append(argv)
        return SimpleNamespace(returncode=0)
    assert schedule.run_target("aifs-ens-probabilities","20261015",12,run)
    assert len(calls)==2
    assert "config/jobs/aifs_ens_probabilities_ser.yaml" in calls[0]
    assert calls[1][-2:]==["--workers","1"]
    assert calls[1][calls[1].index("--source")+1]=="aifs-ens-probabilities"
