import json
from pathlib import Path
import struct
import importlib.util

import pytest

from climate_download.grib.index import IndexRecord
from climate_download.probability_history import availability, normalize_offsets, sample_catalog, validate_subset


def record(offset, length=24):
    return IndexRecord(param="tpg50", levtype="sfc", step="0-24", _offset=offset, _length=length)


def test_legacy_offsets_rebuilt_before_filtering_and_size_mismatch_refused():
    old = [record(0), record(24), record(0), record(24)]
    fixed, mode = normalize_offsets(old, 96)
    assert [r.offset for r in fixed] == [0, 24, 48, 72]
    assert mode == "cumulative_lengths_verified"
    assert [r.offset for r in old] == [0, 24, 0, 24]
    with pytest.raises(ValueError, match="lengths"):
        normalize_offsets(old, 97)


def test_global_gapped_offsets_are_not_rewritten():
    records = [record(24), record(96)]
    assert normalize_offsets(records, 120) == (records, "original")
    with pytest.raises(ValueError, match="beyond"):
        normalize_offsets(records, 110)


def test_receipt_marks_absent_fields_and_each_missing_window_separately():
    result = availability("aifs-ens", [{"bundle": 240, "available_events": [("tpg50", "0-24")]},
                                      {"bundle": 360, "available_events": []}])
    assert result["status"] == "partial"
    assert result["available_samples"] == {"tpg50": [24]}
    assert "tpg50" not in result["missing_variables"]
    assert len(result["missing_samples"]["tpg50"]) == 28
    assert "10spg10" in result["missing_variables"]
    assert availability("ifs", [{"available_events": []}])["status"] == "unavailable"
    assert len(sample_catalog("aifs-ens")) == 25
    assert sum(map(len, sample_catalog("aifs-ens").values())) == 313


def message(length=24):
    return b"GRIB\0\0\0\2" + struct.pack(">Q", length) + b"0" * (length - 20) + b"7777"


def test_resume_proof_validates_every_message_not_just_outer_markers(tmp_path):
    path = tmp_path / "subset.grib2"
    path.write_bytes(message() + message())
    validate_subset(path, [record(0), record(24)])
    path.write_bytes(message() + message(25))
    with pytest.raises(ValueError, match="length/header"):
        validate_subset(path, [record(0), record(24)])
    path.write_bytes(message() + message())
    with pytest.raises(ValueError, match="extra"):
        validate_subset(path, [record(0)])


def script(name):
    path=Path(__file__).parents[1]/name
    spec=importlib.util.spec_from_file_location(path.stem,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    return module


def test_queue_keeps_the_whole_init_gap_after_earliest_archive():
    module=script("scripts/build_probability_history_queue.py")
    days=[dict(date="20250701",objects=[dict(name=f"20250701/{c:02}z/aifs-ens/0p25/enfo/x.index") for c in (6,18)])]
    queue=module.build_queue(days)
    assert queue["object_inits"]["aifs-ens"]==2
    assert queue["target_inits"]["aifs-ens"]==3
    assert queue["upstream_object_gaps"]==[dict(model="aifs-ens",date="20250701",cycle=12)]
    assert queue["inits"][0]["cycle"]==6


def test_old_model_directory_and_coarse_archive_choose_finest_complete_product():
    module=script("scripts/build_probability_history_queue.py")
    def objects(date,layout):
        return [dict(name=f'{date}/12z/{layout}/enfo/{date}120000-{b}h-enfo-ep.{s}')
            for b in (240,360) for s in ('index','grib2')]
    queue=module.build_queue([dict(date='20230712',objects=objects('20230712','0p4-beta')),
        dict(date='20230713',objects=objects('20230713','0p4-beta')+objects('20230713','0p25'))])
    assert queue['inits'][0]['layout']=='0p4-beta'
    assert queue['inits'][0]['grid_resolution_degrees']==0.4
    assert queue['inits'][-1]['layout']=='0p25'
    assert queue['inits'][-1]['grid_resolution_degrees']==0.25


def test_runner_finishes_after_last_pass_and_keeps_partial_warnings(tmp_path,monkeypatch):
    from datetime import datetime,timezone
    from types import SimpleNamespace
    runner=script("deploy/probability_history_run.py")
    queue=tmp_path/"queue.json"
    queue.write_text(json.dumps(dict(inits=[dict(model="ifs",date="20240228",cycle=12)])))
    monkeypatch.setattr(runner.time,"sleep",lambda n:None)
    monkeypatch.setattr(runner.shutil,"disk_usage",lambda p:SimpleNamespace(free=200*1024**3))
    monkeypatch.setattr(runner,"run_one",lambda item,log:dict(download=dict(status="partial",bytes=10,
        missing_samples={"tpg50":[24,36]}),restore=dict(status="done")))
    state=tmp_path/"state"
    assert runner.main(["--queue",str(queue),"--state-root",str(state),"--lock",str(tmp_path/"lock"),
                        "--max-passes","1","--ignore-priority"])==0
    progress=json.loads((state/"progress.json").read_text())
    assert progress["status"]=="complete"
    assert progress["sources"]["ifs-ens-probabilities"]["gaps"][0]["missing_windows"]==2
    assert runner.daily_priority(datetime(2026,10,16,9,15,tzinfo=timezone.utc))
    assert not runner.daily_priority(datetime(2026,10,16,8,15,tzinfo=timezone.utc))


def test_history_process_deadline_leaves_the_daily_schedule_priority_margin():
    runner=script("deploy/probability_history_run.py")
    command=runner.inside('/workspace/climate_down',['python','scripts/backfill_probability_init.py'])
    assert command.index('timeout') < command.index('uv')
    phase=int(command[command.index('--kill-after=15s')+1].removesuffix('s'))
    assert 2*(phase+15)<15*60


def test_a_limited_acceptance_run_cannot_mark_the_full_queue_complete(tmp_path,monkeypatch):
    from types import SimpleNamespace
    runner=script('deploy/probability_history_run.py')
    queue=tmp_path/'queue.json'
    queue.write_text(json.dumps(dict(inits=[dict(model='ifs',date=d,cycle=12) for d in ('20230712','20230713')])))
    monkeypatch.setattr(runner.time,'sleep',lambda n:None)
    monkeypatch.setattr(runner.shutil,'disk_usage',lambda p:SimpleNamespace(free=200*1024**3))
    monkeypatch.setattr(runner,'run_one',lambda item,log:dict(download=dict(status='complete',bytes=1),restore=dict(status='done')))
    common=['--queue',str(queue),'--state-root',str(tmp_path/'state'),'--lock',str(tmp_path/'lock'),'--ignore-priority']
    assert runner.main(common+['--limit','1'])==0
    assert runner.main(common)==0
    p=json.loads((tmp_path/'state/progress.json').read_text())
    assert p['sources']['ifs-ens-probabilities']['counts']=={'done':2}
    assert not p['limited']
