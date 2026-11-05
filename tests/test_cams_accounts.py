"""Account ownership, bounded admission and three-lane historical scheduling."""
import json
import multiprocessing
from pathlib import Path
from types import SimpleNamespace

import pytest

from climate_download.cams import backfill, worker
from climate_download.cams.catalogue import request_for
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import digest, save, signature


@pytest.fixture
def pool(tmp_path, monkeypatch):
    cfg = CamsConfig(output_dir=tmp_path/'data', first_init='2026102100',
                     accounts=[{'name': n, 'credential_file': tmp_path/n} for n in ('a', 'b', 'c')],
                     cycle_accounts={0: 'a', 12: 'b'}, history_accounts=['a', 'b', 'c'],
                     max_active_orders_per_account=1)
    plan = backfill.make_plan('20260101', '20260921', authorized_at='2026-11-05T10:00:00Z')
    root = tmp_path/'queue'
    save(root/'plan.json', plan)
    monkeypatch.setattr(backfill, 'pending_inits', lambda *a: [])
    monkeypatch.setattr(backfill.shutil, 'disk_usage', lambda *a: (0, 0, 10**13))
    clients = {n: SimpleNamespace(get_jobs=lambda **kw: SimpleNamespace(json={'jobs': []}))
               for n in ('a', 'b', 'c')}
    monkeypatch.setattr(backfill, 'create_client', lambda account: (clients[account.name], ''))
    return cfg, plan, root, clients


def retained(cfg, key, account, phase='submitted', rid='original'):
    folder = cfg.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')/'.cams/hourly'
    request = request_for(key[:8], int(key[8:]), 'hourly')
    save(folder/'request.json', request)
    save(folder/'state.json', {'date': key[:8], 'cycle': int(key[8:]), 'group': 'hourly',
         'account': account, 'dataset': worker.DATASET, 'phase': phase, 'request_id': rid,
         'request_sha256': signature(request)})
    return folder


def test_three_workers_claim_different_inits_and_reuse_original_accounts(pool, monkeypatch):
    cfg, plan, root, _ = pool
    plan_before = digest(root/'plan.json')
    seen = []
    def run(scoped, date, cycle, **kw):
        seen.append((scoped.account_for(cycle).name, date+f'{cycle:02d}', kw))
        assert scoped._realtime_config is cfg
        assert kw['require_idle_account']
        return {'status': 'pending'}
    monkeypatch.setattr(backfill, 'run_init', run)
    for account in ('a', 'b', 'c'):
        backfill.tick(cfg, root, account=account)
    assert [s[0] for s in seen] == ['a', 'b', 'c']
    assert len({s[1] for s in seen}) == 3
    originals = [(a, k) for a, k, _ in seen]
    for account in ('a', 'b', 'c'):
        backfill.tick(cfg, root, account=account)
    assert [(a, k) for a, k, _ in seen[3:]] == originals
    assert digest(root/'plan.json') == plan_before
    assert cfg.cycle_accounts == {0: 'a', 12: 'b'}


@pytest.mark.parametrize('phase', ['submitted', 'submitting', 'submission_uncertain', 'remote_failed', 'complete'])
def test_third_account_never_takes_retained_requests_or_failures(pool, monkeypatch, phase):
    cfg, plan, root, _ = pool
    key = plan['weeks'][0]['inits'][0]
    folder = retained(cfg, key, 'a', phase)
    before = {p.name: p.read_bytes() for p in folder.iterdir()}
    seen = []
    monkeypatch.setattr(backfill, 'run_init', lambda sc, d, c, **kw: seen.append(d+f'{c:02d}') or {'status': 'pending'})
    backfill.tick(cfg, root, account='c')
    assert seen and key not in seen
    assert {p.name: p.read_bytes() for p in folder.iterdir()} == before


def test_opaque_old_bytes_keep_the_original_cycle_account(pool, monkeypatch):
    cfg, plan, root, _ = pool
    key = plan['weeks'][0]['inits'][0]
    path = cfg.output_dir/'ec_cams'/key[:8]/'00z/.cams/hourly/download.grib.part'
    path.parent.mkdir(parents=True)
    path.write_bytes(b'old evidence without a receipt')
    seen = []
    monkeypatch.setattr(backfill, 'run_init', lambda sc, d, c, **kw: seen.append(d+f'{c:02d}') or {'status': 'pending'})
    backfill.tick(cfg, root, account='c')
    assert key not in seen and path.read_bytes() == b'old evidence without a receipt'


def test_existing_active_order_is_resumed_before_an_unsubmitted_init(pool, monkeypatch):
    cfg, plan, root, clients = pool
    key = plan['weeks'][0]['inits'][8]
    retained(cfg, key, 'a')
    clients['a'].get_jobs = lambda **kw: SimpleNamespace(json={'jobs': [{'jobID': 'original'}]})
    seen = []
    monkeypatch.setattr(backfill, 'run_init', lambda sc, d, c, **kw: seen.append(d+f'{c:02d}') or {'status': 'pending'})
    backfill.tick(cfg, root, account='a')
    assert seen == [key]


@pytest.mark.parametrize('jobs', [[{'jobID': 'unknown'}], [{'jobID': 'one'}, {'jobID': 'two'}]])
def test_unknown_or_multiple_active_orders_block_new_claims(pool, monkeypatch, jobs):
    cfg, _, root, clients = pool
    clients['c'].get_jobs = lambda **kw: SimpleNamespace(json={'jobs': jobs})
    monkeypatch.setattr(backfill, 'run_init', lambda *a, **kw: pytest.fail('account must wait'))
    assert backfill.tick(cfg, root, account='c')['status'] == 'waiting_for_account'
    assert not list((root/'assignments').glob('*.json'))


def test_either_realtime_cycle_prevents_third_account_claim(pool, monkeypatch):
    cfg, _, root, _ = pool
    monkeypatch.setattr(backfill, 'pending_inits', lambda config, cycle, at: ['2026110500'] if cycle == 0 else [])
    monkeypatch.setattr(backfill, 'run_init', lambda *a, **kw: pytest.fail('realtime wins'))
    assert backfill.tick(cfg, root, account='c')['status'] == 'waiting_for_realtime'
    assert not list((root/'assignments').glob('*.json'))


def test_metadata_failure_cannot_create_an_assignment(pool, monkeypatch):
    cfg, _, root, clients = pool
    def unavailable(**kwargs):
        raise TimeoutError('metadata unavailable')
    clients['c'].get_jobs = unavailable
    monkeypatch.setattr(backfill, 'run_init', lambda *a, **kw: pytest.fail('metadata must be verified'))
    assert backfill.tick(cfg, root, account='c')['status'] == 'waiting_for_account'
    assert not list((root/'assignments').glob('*.json'))


def test_assignment_cannot_be_reassigned_or_detached_from_plan(pool):
    from climate_download.cams import allocation
    cfg, plan, root, _ = pool
    key = plan['weeks'][0]['inits'][0]
    assert allocation.claim(cfg, root, plan, key, 'c')
    assert not allocation.claim(cfg, root, plan, key, 'a')
    path = root/'assignments'/(key+'.json')
    value = json.loads(path.read_text())
    value['plan_signature'] = 'wrong-plan'
    save(path, value)
    with pytest.raises(ValueError, match='assignment'):
        allocation.owner(cfg, root, plan, key)


def test_assignment_conflict_with_original_account_is_not_repaired_silently(pool):
    from climate_download.cams import allocation
    cfg, plan, root, _ = pool
    key = plan['weeks'][0]['inits'][0]
    assert allocation.claim(cfg, root, plan, key, 'c')
    folder = retained(cfg, key, 'a')
    before = (folder/'state.json').read_bytes()
    with pytest.raises(ValueError, match='assignment'):
        allocation.owner(cfg, root, plan, key)
    assert (folder/'state.json').read_bytes() == before


def test_claim_survives_a_crash_between_request_file_and_intent(pool):
    from climate_download.cams import allocation
    cfg, plan, root, _ = pool
    key = plan['weeks'][0]['inits'][0]
    assert allocation.claim(cfg, root, plan, key, 'c')
    folder = cfg.output_dir/'ec_cams'/key[:8]/'00z/.cams/hourly'
    save(folder/'request.json', request_for(key[:8], 0, 'hourly'))
    assert allocation.owner(cfg, root, plan, key) == 'c'
    assert allocation.claim(cfg, root, plan, key, 'c')
    assert not allocation.claim(cfg, root, plan, key, 'a')


def _claim_one(config_data, queue, plan, account, results):
    from climate_download.cams.allocation import claim
    cfg = CamsConfig.model_validate(config_data)
    for key in plan['weeks'][0]['inits']:
        if claim(cfg, Path(queue), plan, key, account):
            results.put((account, key))
            return


def test_actual_processes_cannot_claim_the_same_init(pool):
    cfg, plan, root, _ = pool
    context = multiprocessing.get_context('spawn')
    results = context.Queue()
    processes = [context.Process(target=_claim_one, args=(cfg.model_dump(), str(root), plan, a, results))
                 for a in ('a', 'b', 'c')]
    for process in processes:
        process.start()
    for process in processes:
        process.join(20)
        assert process.exitcode == 0
    claims = [results.get(timeout=2) for _ in processes]
    assert len({key for _, key in claims}) == 3


def test_post_gate_rechecks_active_orders_after_preflight(pool, monkeypatch):
    cfg, _, _, _ = pool
    request = request_for('20261105', 0, 'hourly')
    client = SimpleNamespace(submit=lambda *a: pytest.fail('second order forbidden'),
                             get_jobs=lambda **kw: SimpleNamespace(json={'jobs': [{'jobID': 'busy'}]}))
    monkeypatch.setattr(worker, 'preflight', lambda *a: {'passed': True})
    folder = cfg.output_dir/'group'
    assert not worker.group_tick(cfg, folder, '20261105', 0, 'hourly', client, '')
    saved = json.loads((folder/'state.json').read_text())
    assert not saved.get('request_id') and not saved.get('retry_count')


def test_new_realtime_after_preflight_blocks_historical_post(pool, monkeypatch):
    cfg, _, _, _ = pool
    scoped = cfg.model_copy(update={'first_init': '2026010100'})
    scoped._realtime_config = cfg
    client = SimpleNamespace(submit=lambda *a: pytest.fail('realtime arrived'),
                             get_jobs=lambda **kw: SimpleNamespace(json={'jobs': []}))
    monkeypatch.setattr(worker, 'preflight', lambda *a: {'passed': True})
    monkeypatch.setattr(worker, 'pending_inits', lambda config, cycle, at: ['2026110500'] if cycle == 0 else [])
    folder = cfg.output_dir/'group'
    assert not worker.group_tick(scoped, folder, '20260915', 12, 'hourly', client, '')
    assert json.loads((folder/'state.json').read_text())['retry_wait_reason'] == 'newer_live_due'


def test_busy_account_does_not_consume_a_replacement(pool, monkeypatch):
    cfg, _, _, _ = pool
    folder = retained(cfg, '2026110500', 'a', 'retry_wait')
    original = json.loads((folder/'state.json').read_text())
    original.update(retry_count=7, previous_attempts=[{'request_id': 'preserved-failure'}])
    save(folder/'state.json', original)
    client = SimpleNamespace(submit=lambda *a: pytest.fail('account busy'),
                             get_jobs=lambda **kw: SimpleNamespace(json={'jobs': [{'jobID': 'other'}]}))
    monkeypatch.setattr(worker.recovery, 'retry_decision', lambda *a: 'retry')
    monkeypatch.setattr(worker, 'preflight', lambda *a: {'passed': True})
    monkeypatch.setattr(worker.recovery, 'verify_before_replacement', lambda *a: pytest.fail('no budget use'))
    assert not worker.group_tick(cfg, folder, '20261105', 0, 'hourly', client, '')
    saved = json.loads((folder/'state.json').read_text())
    assert saved['retry_count'] == 7 and saved['previous_attempts'] == original['previous_attempts']


def test_realtime_host_uses_the_committed_runtime_and_keeps_cycle_ownership(tmp_path, monkeypatch):
    import importlib.util
    spec = importlib.util.spec_from_file_location('account_live_host', Path(__file__).parents[1]/'deploy/cams_run.py')
    live_host = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(live_host)
    monkeypatch.setenv('CAMS_OPERATION_DIR', str(tmp_path/'live'))
    def run(command, **kwargs):
        assert command[command.index('-w')+1] == '/workspace/releases/test'
        assert 'PYTHONPATH=/workspace/releases/test/src' in command
        assert command[-3:] == ['--cycle', '0', '--scheduled']
        return SimpleNamespace(returncode=0, stdout='complete', stderr='')
    monkeypatch.setattr(live_host.subprocess, 'run', run)
    assert live_host.main(['--cycle', '0', '--runtime-root', '/workspace/releases/test']) == 0


def test_legacy_live_and_history_orders_drain_without_adding_another_order(pool):
    cfg, _, _, _ = pool
    client = SimpleNamespace(submit=lambda *a: pytest.fail('do not add a third legacy order'),
        get_jobs=lambda **kw: SimpleNamespace(json={'jobs': [{'jobID': 'old-history'}, {'jobID': 'old-live'}]}))
    result = worker.run_init(cfg, '20261105', 0, client=client)
    assert result['status'] == 'waiting_for_account'
    assert not list(cfg.output_dir.glob('ec_cams/20261105/00z/.cams/*/state.json'))
