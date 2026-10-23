"""CAMS failure contracts: no partial publication or duplicate ADS submission."""
from dataclasses import replace
import datetime as dt
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from climate_download.cams.catalogue import DATASET, EXPECTED_MESSAGES, GROUPS, SOURCE, VARIABLES, request_for
from climate_download.cams.config import CamsConfig, candidate_inits
from climate_download.cams import grib, worker
from climate_download.cams.state import save, signature, safe_error


@pytest.fixture
def config(tmp_path):
    return CamsConfig(output_dir=tmp_path, accounts=[{"name": "a", "credential_file": tmp_path / "credential"}],
                      cycle_accounts={0: "a", 12: "a"}, first_init="2026102100", max_run_seconds=0)


def test_frozen_native_inventory_and_three_safe_requests():
    assert SOURCE == "ec_cams"
    assert DATASET == "cams-global-atmospheric-composition-forecasts"
    assert len(VARIABLES) == 52 and len({v.param_id for v in VARIABLES}) == 52
    assert EXPECTED_MESSAGES == 5732
    assert {k: sum(len(v.leads) for v in vs) for k, vs in GROUPS.items()} == {
        "hourly": 5445, "gust": 41, "pressure850": 246}
    for group, variables in GROUPS.items():
        request = request_for("20261021", 0, group)
        assert request["leadtime_hour"][-1] == "120"
        assert request["area"] == [55, 70, 15, 140]
        assert len(request["leadtime_hour"]) == len(variables[0].leads)
        assert ("pressure_level" in request) == (group == "pressure850")


def test_scheduler_utc_floor_and_published_only(config):
    utc = dt.timezone.utc
    assert candidate_inits(config, 0, dt.datetime(2026, 10, 22, 9, 59, tzinfo=utc)) == ["2026102100"]
    assert candidate_inits(config, 0, dt.datetime(2026, 10, 22, 10, tzinfo=utc)) == ["2026102100", "2026102200"]
    assert candidate_inits(config, 12, dt.datetime(2026, 10, 21, 21, 59, tzinfo=utc)) == []
    assert candidate_inits(config, 12, dt.datetime(2026, 10, 21, 22, tzinfo=utc)) == ["2026102112"]
    with pytest.raises(ValueError, match="predates"):
        worker.run_init(config, "20261020", 0, client=object())


def test_scheduled_scan_leaves_explicit_backfill_to_its_original_worker(config):
    path = config.output_dir / SOURCE / "20261020/00z/.cams/hourly/state.json"
    save(path, {"date": "20261020", "cycle": 0, "phase": "downloading",
                "account": "a", "request_id": "existing-backfill-job"})
    before = path.read_bytes()
    assert worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 22, 9, tzinfo=dt.UTC)) == ["2026102100"]
    assert path.read_bytes() == before


def test_scheduled_scan_still_rejects_cycle_ownership_drift(config):
    path = config.output_dir / SOURCE / "20261021/00z/.cams/hourly/state.json"
    save(path, {"date": "20261021", "cycle": 12, "phase": "submitted", "request_id": "wrong-cycle"})
    with pytest.raises(ValueError, match="ownership"):
        worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 22, 9, tzinfo=dt.UTC))


def fake_client():
    remote = SimpleNamespace(request_id="original-job", json={"status": "accepted", "created": "t"})
    class Client:
        submits = 0
        def submit(self, dataset, request):
            self.submits += 1
            return remote
        def get_remote(self, request_id):
            assert request_id == "original-job"
            return remote
    return Client()


def test_pending_same_job_is_reused_and_no_manifest(config, monkeypatch):
    monkeypatch.setattr(worker, "preflight", lambda *a: {"passed": True})
    client = fake_client()
    for _ in range(2):
        assert worker.run_init(config, "20261021", 0, client=client)["status"] == "pending"
    assert client.submits == 1
    assert not list(config.output_dir.rglob("*.manifest.json"))
    state = json.loads(next(config.output_dir.rglob("state.json")).read_text())
    assert state["request_id"] == "original-job" and state["account"] == "a"


@pytest.mark.parametrize("phase", ["submitting", "submission_uncertain", "remote_failed", "validation_failed"])
def test_ambiguous_or_failed_job_never_resubmits(config, tmp_path, phase):
    folder = tmp_path / "group"
    save(folder / "state.json", {"phase": phase, "account": "a", "dataset": worker.DATASET,
                               "request_sha256": signature(request_for("20261021", 0, "hourly"))})
    client = fake_client()
    with pytest.raises(ValueError, match="inspection"):
        worker.group_tick(config, folder, "20261021", 0, "hourly", client, "")
    assert client.submits == 0


def test_lost_submit_response_is_durable_and_redacted(config, tmp_path, monkeypatch):
    monkeypatch.setattr(worker, "preflight", lambda *a: {})
    client = fake_client()
    def lost(*a):
        raise TimeoutError("secret-value https://signed.invalid/file?token=secret-value person@example.org")
    client.submit = lost
    folder = tmp_path / "group"
    with pytest.raises(ValueError, match="uncertain"):
        worker.group_tick(config, folder, "20261021", 0, "hourly", client, "secret-value")
    text = (folder / "state.json").read_text()
    assert "submission_uncertain" in text
    assert "secret-value" not in text and "https://" not in text and "@" not in text
    with pytest.raises(ValueError, match="inspection"):
        worker.group_tick(config, folder, "20261021", 0, "hourly", client, "secret-value")


def test_account_or_request_drift_rejected(config, tmp_path):
    save(tmp_path / "state.json", {"phase": "submitted", "account": "another",
                                  "dataset": worker.DATASET,
                                  "request_sha256": signature(request_for("20261021", 0, "hourly"))})
    with pytest.raises(ValueError, match="Pinned"):
        worker.group_tick(config, tmp_path, "20261021", 0, "hourly", fake_client(), "")


def header(v, h):
    valid = dt.datetime(2026, 10, 21) + dt.timedelta(hours=h)
    return {**grib.GRID, "paramId": v.param_id, "dataDate": 20261021, "dataTime": 0,
            "dataType": "fc", "endStep": h, "stepUnits": 1, "numberOfMissing": 0,
            "startStep": h if v.param_id != 228028 or h == 0 else h - 3,
            "stepType": "max" if v.param_id == 228028 and h else "instant",
            "typeOfLevel": v.type_of_level, "level": v.level, "units": v.units, "shortName": v.short_name,
            "validityDate": int(valid.strftime("%Y%m%d")), "validityTime": int(valid.strftime("%H%M"))}


@pytest.fixture
def raw_messages(tmp_path, monkeypatch):
    messages = [(header(v, h), b"GRIBbytes7777") for v in VARIABLES for h in v.leads]
    path = tmp_path / "raw.grib"
    path.write_bytes(b"".join(m for _, m in messages))
    monkeypatch.setattr(grib, "iter_messages", lambda p: iter(messages))
    return path, messages


def test_publish_complete_native_identity_byte_hash_and_idempotency(raw_messages, tmp_path):
    raw, messages = raw_messages
    folder = tmp_path / "product"
    manifest = grib.publish([raw], folder, "20261021", 0, [])
    payload = json.loads(manifest.read_text())
    assert len(payload["files"]) == 121 and payload["failures"] == []
    assert sum(f["records_selected"] for f in payload["files"]) == 5732
    assert payload["files"][1]["records_selected"] == 45
    assert payload["files"][3]["records_selected"] == 52
    assert grib.verify_published(folder, "20261021", 0)
    before = manifest.stat().st_mtime_ns
    assert grib.publish([raw], folder, "20261021", 0, []) == manifest
    assert manifest.stat().st_mtime_ns == before
    (folder / "f120.subset.grib2").write_bytes(b"damaged")
    with pytest.raises(ValueError, match="no longer matches"):
        grib.verify_published(folder, "20261021", 0)


@pytest.mark.parametrize("problem", ["missing_h120", "duplicate", "extra_gust", "units", "shift", "init", "trailing"])
def test_partial_foreign_or_changed_data_never_publishes(raw_messages, tmp_path, problem):
    raw, messages = raw_messages
    if problem == "missing_h120":
        messages.pop()
    elif problem == "duplicate":
        messages[-1] = messages[0]
    elif problem == "extra_gust":
        messages[-1] = (header(GROUPS["gust"][0], 1), b"GRIBbytes7777")
    elif problem == "units":
        messages[0][0]["units"] = "W m**-2"
    elif problem == "shift":
        messages[0][0]["longitudeOfFirstGridPointInDegrees"] += 0.4
    elif problem == "init":
        messages[0][0]["dataTime"] = 1200
    elif problem == "trailing":
        raw.write_bytes(raw.read_bytes() + b"junk")
    if problem != "trailing":
        raw.write_bytes(b"".join(m for _, m in messages))
    folder = tmp_path / "product"
    with pytest.raises(ValueError):
        grib.publish([raw], folder, "20261021", 0, [])
    assert not list(folder.glob("*.manifest.json"))


def test_same_length_message_reencoding_is_rejected(raw_messages, tmp_path):
    raw, messages = raw_messages
    original = messages[0][1]
    messages[0] = (messages[0][0], b"X" * len(original))
    with pytest.raises(ValueError, match="message bytes differ"):
        grib.publish([raw], tmp_path / "product", "20261021", 0, [])


def test_preflight_is_read_only_and_native_complete():
    from climate_download.cams.client import preflight
    variables = GROUPS["hourly"]
    constraints = [{"variable": [v.api_name for v in variables], "type": ["forecast"],
                    "date": ["2026-10-01/2026-10-31"], "time": ["00:00", "12:00"],
                    "leadtime_hour": [str(h) for h in range(121)]}]
    client = SimpleNamespace(get_jobs=lambda **k: [],
        _profile_api=SimpleNamespace(accepted_licences=lambda: {"licences": [{"id": "cc-by", "revision": 1}]}),
        get_collection=lambda d: SimpleNamespace(constraints=constraints),
        estimate_costs=lambda *a: {"cost": 5445, "limit": 10000})
    assert preflight(client, request_for("20261021", 0, "hourly"), "hourly")["cost"] == 5445
    constraints[0]["leadtime_hour"].pop()
    with pytest.raises(ValueError, match="native leads"):
        preflight(client, request_for("20261021", 0, "hourly"), "hourly")


def test_resume_saved_init_when_recent_submission_window_has_moved(config):
    folder = config.output_dir / "ec_cams/20261021/00z/.cams/hourly"
    save(folder / "state.json", {"request_id": "original-job", "phase": "waiting_for_remote",
                                "date": "20261021", "cycle": 0})
    keys = worker.scheduled_inits(config, 0, dt.datetime(2026, 10, 25, 12, tzinfo=dt.UTC))
    assert keys == ["2026102100", "2026102400", "2026102500"]


def test_transfer_does_not_log_or_put_signed_url_in_argv(config, tmp_path, monkeypatch):
    from climate_download.cams import client as module
    part = tmp_path / "file.part"
    part.write_bytes(b"ABC")
    remote = SimpleNamespace(get_results=lambda: SimpleNamespace(
        asset={"href": "https://asset.invalid/data?token=do-not-log", "file:size": 6}))
    def curl(argv, **kwargs):
        assert "https://" not in " ".join(argv) and "do-not-log" not in " ".join(argv)
        assert "--continue-at" in argv and "--config" in argv
        assert "do-not-log" in kwargs["input"]
        part.write_bytes(b"ABCDEF")
        return SimpleNamespace(returncode=0, stdout=json.dumps({"http_code": 206,
            "url_effective": "https://asset.invalid/?token=do-not-log"}))
    monkeypatch.setattr(module.subprocess, "run", curl)
    receipt, done = module.transfer(remote, part, config)
    assert done and "do-not-log" not in json.dumps(receipt)
    with pytest.raises(ValueError, match="length changed"):
        module.transfer(remote, part, config, expected_bytes=7)


def test_terms_or_cost_rejection_fails_loudly_without_submitting(config, tmp_path, monkeypatch):
    from climate_download.cams.client import PreflightRejected
    def reject(*args):
        raise PreflightRejected("ADS licence not accepted")
    monkeypatch.setattr(worker, "preflight", reject)
    client = fake_client()
    folder = tmp_path / "group"
    with pytest.raises(ValueError, match="preflight rejected"):
        worker.group_tick(config, folder, "20261021", 0, "hourly", client, "")
    assert client.submits == 0
    assert json.loads((folder / "state.json").read_text())["phase"] == "preflight_rejected"


def test_credential_parser_exception_is_never_rendered(monkeypatch):
    from climate_download.cams import client as module
    def malformed(*args):
        raise ValueError("invalid YAML line: key: sensitive-account-key")
    monkeypatch.setattr(module, "_create_client", malformed)
    with pytest.raises(ValueError) as caught:
        module.create_client(object())
    assert "sensitive-account-key" not in str(caught.value)
    assert "Unable to initialize ADS client" in str(caught.value)


def test_blocked_job_does_not_refresh_a_false_progress_signal(config):
    folder = config.output_dir / 'ec_cams/20261021/00z'
    save(folder / '.cams/hourly/state.json', {'phase': 'validation_failed', 'account': 'a',
         'dataset': worker.DATASET, 'request_sha256': signature(request_for('20261021', 0, 'hourly'))})
    progress = folder / 'cams.status.json'
    save(progress, {'phase': 'downloading', 'last_check': 'earlier'})
    before = progress.stat().st_mtime_ns
    with pytest.raises(ValueError, match='inspection'):
        worker.run_init(config, '20261021', 0, client=fake_client())
    assert progress.stat().st_mtime_ns == before


def test_inaccessible_saved_job_is_reported_without_resubmission(config, tmp_path):
    client = fake_client()
    def denied(*args):
        error = RuntimeError('403 token=private-key')
        error.response = SimpleNamespace(status_code=403)
        raise error
    client.get_remote = denied
    save(tmp_path / 'state.json', {'phase': 'submitted', 'account': 'a', 'dataset': worker.DATASET,
         'request_id': 'original-job', 'request_sha256': signature(request_for('20261021', 0, 'hourly'))})
    with pytest.raises(ValueError, match='inaccessible'):
        worker.group_tick(config, tmp_path, '20261021', 0, 'hourly', client, 'private-key')
    state = json.loads((tmp_path / 'state.json').read_text())
    assert state['phase'] == 'remote_failed' and state['request_id'] == 'original-job'
    assert client.submits == 0 and 'private-key' not in json.dumps(state)
