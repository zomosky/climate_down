"""Drain retained successful history while the same account serves live work."""
from contextlib import ExitStack
import datetime as dt
import fcntl
import json
from types import SimpleNamespace

import pytest

from climate_download.cams import backfill, worker
from climate_download.cams.catalogue import request_for
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import digest, save, signature
from test_cams_staged_e2e import system, report


@pytest.mark.parametrize("phase", ["prepared", "retry_wait", "submission_uncertain", "remote_failed", "validation_failed"])
def test_completed_only_never_changes_unsubmitted_or_failed_records(tmp_path, phase):
    cfg = CamsConfig(output_dir=tmp_path, accounts=[{"name": "a", "credential_file": tmp_path/"unused"}],
                     cycle_accounts={0: "a", 12: "a"}, first_init="2026010100")
    folder = tmp_path/"group"
    request = request_for("20260716", 0, "hourly")
    save(folder/"request.json", request)
    save(folder/"state.json", {"phase": phase, "account": "a", "dataset": worker.DATASET,
        "date": "20260716", "cycle": 0, "group": "hourly", "request_id": "retained",
        "request_sha256": signature(request), "retry_count": 8,
        "previous_attempts": [{"request_id": "old-failure"}]})
    before = {p.name: p.read_bytes() for p in folder.iterdir()}
    assert not worker.group_tick(cfg, folder, "20260716", 0, "hourly", object(), "", completed_only=True)
    assert {p.name: p.read_bytes() for p in folder.iterdir()} == before


def test_completed_only_does_not_create_a_prepared_request(tmp_path):
    cfg = CamsConfig(output_dir=tmp_path, accounts=[{"name": "a", "credential_file": tmp_path/"unused"}],
                     cycle_accounts={0: "a", 12: "a"}, first_init="2026010100")
    folder = tmp_path/"absent"
    assert not worker.group_tick(cfg, folder, "20260716", 0, "hourly", object(), "", completed_only=True)
    assert not folder.exists()


@pytest.mark.parametrize("remote_status", ["accepted", "running", "failed", "deleted"])
def test_completed_only_waits_for_success_without_enrolling_a_retry(tmp_path, remote_status):
    cfg = CamsConfig(output_dir=tmp_path, accounts=[{"name": "a", "credential_file": tmp_path/"unused"}],
                     cycle_accounts={0: "a", 12: "a"}, first_init="2026010100")
    folder = tmp_path/"group"
    save(folder/"state.json", {"phase": "waiting_for_remote", "account": "a", "dataset": worker.DATASET,
        "date": "20260716", "cycle": 0, "group": "hourly", "request_id": "retained",
        "request_sha256": signature(request_for("20260716", 0, "hourly"))})
    before = (folder/"state.json").read_bytes()
    client = SimpleNamespace(get_remote=lambda rid: SimpleNamespace(json={"status": remote_status}))
    assert not worker.group_tick(cfg, folder, "20260716", 0, "hourly", client, "", completed_only=True)
    assert (folder/"state.json").read_bytes() == before


@pytest.mark.parametrize('live_mode', ['submit_during_transfer', 'already_running'])
@pytest.mark.parametrize('partial_restart', [False, True])
def test_successful_history_finishes_while_live_submits_on_the_same_account(system, monkeypatch, live_mode, partial_restart):
    original, clock, client, tmp = system
    cfg = original.model_copy(update={"first_init": "2026102712", "max_run_seconds": 0,
        "hourly_request_mode": "bundle", "history_accounts": ["mock-account"], "max_active_orders_per_account": 1})
    client.failures = 0
    plan = backfill.make_plan("20260101", "20260722", authorized_at=clock[0].isoformat())
    root = tmp/"queue"
    save(root/"plan.json", plan)
    monkeypatch.setattr(backfill, "create_client", lambda account: (client, ""))
    def jobs(**kwargs):
        return SimpleNamespace(json={"jobs": [{"jobID": rid, "status": remote.json["status"]}
            for rid, remote in client.remotes.items() if remote.json["status"] in ("accepted", "running")]})
    client.get_jobs = jobs
    submit = client.submit
    def post(dataset, request):
        assert not jobs().json["jobs"], "maximum one active order per account"
        remote = submit(dataset, request)
        if request["date"] == "2026-10-27" or request.get("pressure_level"):
            remote.json["status"] = "accepted"
        return remote
    client.submit = post
    started = backfill.tick(cfg, root, account="mock-account")
    assert started["status"] == "pending" and started["init"] == "2026071600"
    assert len(client.posts) == 3
    folder = cfg.output_dir/"ec_cams/20260716/00z"
    pressure = json.loads((folder/".cams/pressure850/state.json").read_text())
    ids = [p["request_id"] for p in client.posts]
    client.remotes[pressure["request_id"]].json["status"] = "successful"
    evidence = folder/".cams/hourly/failures/old/state.json"
    save(evidence, {"account": "mock-account", "phase": "remote_failed", "request_id": "archived-failure"})
    before_failure, before_plan = evidence.read_bytes(), digest(root/"plan.json")
    clock[0] += dt.timedelta(hours=12)
    transfer = worker.transfer
    live_runs = []
    if live_mode == 'already_running':
        live_runs.append(worker.run_init(cfg, '20261027', 12, client=client))
        assert live_runs[0]['status'] == 'pending'
    pressure_transfers = []
    def transfer_with_live(remote, part, *args, **kwargs):
        if remote.request_id == pressure["request_id"] and not live_runs:
            live = worker.run_init(cfg, "20261027", 12, client=client)
            live_runs.append(live)
            assert live["status"] == "pending", "historical transfer must leave the account lock free"
        receipt, done = transfer(remote, part, *args, **kwargs)
        if remote.request_id == pressure['request_id']:
            pressure_transfers.append(receipt)
            if partial_restart and len(pressure_transfers) == 1:
                data = part.read_bytes()
                part.write_bytes(data[:len(data)//2])
                receipt['partial_bytes'] = part.stat().st_size
                return receipt, False
        return receipt, done
    monkeypatch.setattr(worker, "transfer", transfer_with_live)
    trace = [started]
    with ExitStack() as locks:
        if live_mode == 'already_running':
            lock = locks.enter_context((cfg.output_dir/'ec_cams/.locks/account-mock-account.lock').open('a'))
            fcntl.flock(lock, fcntl.LOCK_EX|fcntl.LOCK_NB)
        result = backfill.tick(cfg, root, account="mock-account")
        if partial_restart:
            trace.append(result)
            assert result['status'] == 'pending' and result['completed_only'] and result['drained']
            assert len(client.posts) == 4 and not list(folder.glob('*.manifest.json'))
            result = backfill.tick(cfg, root, account='mock-account')
    assert result["status"] == "complete" and result["init"] == "2026071600"
    assert len(live_runs) == 1 and len(client.posts) == 4
    manifest = json.loads((folder/"20260716_00z_ec_cams.manifest.json").read_text())
    assert len(manifest["variables"]) == 52 and len(manifest["files"]) == 121
    assert sum(f["records_selected"] for f in manifest["files"]) == 5732
    assert [r["request_id"] for r in manifest["original_requests"]] == ids
    assert evidence.read_bytes() == before_failure and digest(root/"plan.json") == before_plan
    resumed = backfill.tick(cfg, root, account="mock-account")
    assert resumed["status"] == "waiting_for_realtime" and len(client.posts) == 4
    assert len(jobs().json["jobs"]) == 1
    report(f'completed-history-{live_mode}-{partial_restart}', cfg, client, [*trace, *live_runs, result, resumed], {
        "history_init": "2026071600", "original_request_ids": ids, "original_records_preserved": True,
        "history_additional_submissions": 0,
        'live_submitted_during_history_transfer': live_mode == 'submit_during_transfer',
        'live_account_lock_held_during_history_transfer': live_mode == 'already_running',
        'live_mode': live_mode, 'partial_restart': partial_restart,
        "messages": 5732, "variables": 52, "steps": 121, "max_active_orders_per_account": 1})


def test_completed_only_respects_the_existing_init_lock(tmp_path):
    cfg = CamsConfig(output_dir=tmp_path, accounts=[{'name': 'a', 'credential_file': tmp_path/'unused'}],
                     cycle_accounts={0: 'a', 12: 'a'}, first_init='2026010100')
    locks = tmp_path/'ec_cams/.locks'
    locks.mkdir(parents=True)
    with (locks/'init-2026071600.lock').open('a') as held:
        fcntl.flock(held, fcntl.LOCK_EX|fcntl.LOCK_NB)
        assert worker.run_init(cfg, '20260716', 0, client=object(), completed_only=True)['status'] == 'locked'


def test_completed_only_checks_the_original_remote_request(tmp_path):
    cfg = CamsConfig(output_dir=tmp_path, accounts=[{'name': 'a', 'credential_file': tmp_path/'unused'}],
                     cycle_accounts={0: 'a', 12: 'a'}, first_init='2026010100')
    folder = tmp_path/'group'
    request = request_for('20260716', 0, 'hourly')
    save(folder/'request.json', request)
    save(folder/'state.json', {'phase': 'waiting_for_remote', 'account': 'a', 'dataset': worker.DATASET,
        'request_id': 'retained', 'request_sha256': signature(request)})
    before = (folder/'state.json').read_bytes()
    client = SimpleNamespace(get_remote=lambda rid: SimpleNamespace(json={
        'status': 'successful', 'metadata': {'request': {'ids': request_for('20260717', 0, 'hourly')}}}))
    with pytest.raises(worker.BlockedRequestError, match='differs'):
        worker.group_tick(cfg, folder, '20260716', 0, 'hourly', client, '', completed_only=True)
    assert (folder/'state.json').read_bytes() == before


def test_completed_only_drains_a_saved_later_native_piece_without_submitting_earlier_pieces(system):
    from climate_download.cams import partition
    from test_cams_partition import constraint_rows
    cfg, clock, client, tmp = system
    client.failures = 0
    folder = cfg.output_dir/'ec_cams/20261027/00z/.cams/hourly'
    plan = partition.make_plan('20261027', 0, constraint_rows(0))
    save(folder/'partition.json', plan)
    original = request_for('20261027', 0, 'hourly')
    save(folder/'request.json', original)
    save(folder/'state.json', {'phase': 'partitioned', 'partitioned_request': True,
        'partition_sha256': digest(folder/'partition.json'), 'account': 'mock-account', 'dataset': worker.DATASET,
        'request_sha256': signature(original), 'date': '20261027', 'cycle': 0, 'group': 'hourly'})
    first = plan['parts'][0]
    first_state = folder/'parts'/first['id']/'state.json'
    save(first_state, {'phase': 'prepared', 'account': 'mock-account', 'dataset': worker.DATASET,
        'request_sha256': signature(first['request']), 'date': '20261027', 'cycle': 0, 'group': 'hourly'})
    unsubmitted = first_state.read_bytes()
    part = plan['parts'][-1]
    remote = client.submit(worker.DATASET, part['request'])
    child = folder/'parts'/part['id']
    save(child/'request.json', part['request'])
    save(child/'state.json', {'phase': 'waiting_for_remote', 'account': 'mock-account', 'dataset': worker.DATASET,
        'request_id': remote.request_id, 'request_sha256': signature(part['request']),
        'date': '20261027', 'cycle': 0, 'group': 'hourly'})
    before = digest(folder/'partition.json')
    assert not partition.tick(cfg, folder, '20261027', 0, client, '', completed_only=True)
    assert len(client.posts) == 1 and json.loads((child/'state.json').read_text())['phase'] == 'complete'
    assert json.loads((folder/'state.json').read_text())['parts_complete'] == 1
    assert all(not (folder/'parts'/p['id']).exists() for p in plan['parts'][1:-1])
    assert first_state.read_bytes() == unsubmitted
    assert json.loads((folder/'state.json').read_text())['phase'] == 'partitioned'
    assert not partition.tick(cfg, folder, '20261027', 0, client, '', completed_only=True)
    assert len(client.posts) == 1 and first_state.read_bytes() == unsubmitted
    assert digest(folder/'partition.json') == before
