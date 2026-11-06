"""Actual scheduler/worker/locks/native publication, with a fake ADS service."""
import json
import datetime as dt
import os
from types import SimpleNamespace

from climate_download.cams import backfill, worker
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import save
from test_cams_staged_e2e import system, report


def test_three_accounts_restart_without_duplicate_orders_and_publish_native_inits(system, monkeypatch):
    original, clock, seed, tmp = system
    settings = original.model_dump()
    settings.update(first_init='2026102712', max_run_seconds=0, hourly_request_mode='bundle',
                    accounts=[{'name': n, 'credential_file': tmp/n} for n in ('a', 'b', 'c')],
                    cycle_accounts={0: 'a', 12: 'b'}, history_accounts=['a', 'b', 'c'],
                    max_active_orders_per_account=1)
    config = CamsConfig.model_validate(settings)
    plan = backfill.make_plan('20260101', '20260921', authorized_at='2026-11-05T10:00:00Z')
    root = tmp/'three-account-queue'
    save(root/'plan.json', plan)
    clients, finish, trace = {}, {}, []
    for name in ('a', 'b', 'c'):
        client = type(seed)()
        client.failures = 0
        finish[name] = False
        def jobs(client=client, **kwargs):
            return SimpleNamespace(json={'jobs': [{'jobID': key, 'status': remote.json['status']}
                for key, remote in client.remotes.items() if remote.json['status'] in ('accepted', 'running')]})
        submit = client.submit
        def post(dataset, request, *, name=name, client=client, submit=submit, jobs=jobs):
            assert not jobs().json['jobs'], 'one in-flight order per account'
            remote = submit(dataset, request)
            old = remote.request_id
            remote.request_id = name+'-'+old
            client.remotes[remote.request_id] = client.remotes.pop(old)
            client.posts[-1]['request_id'] = remote.request_id
            if not finish[name]:
                remote.json['status'] = 'accepted'
            return remote
        client.submit, client.get_jobs = post, jobs
        clients[name] = client
    monkeypatch.setattr(backfill, 'create_client', lambda account: (clients[account.name], ''))

    first = {}
    for name in clients:
        result = backfill.tick(config, root, account=name)
        trace.append({'account': name, **result})
        first[name] = (result['init'], clients[name].posts[0]['request_id'])
        assert result['status'] == 'pending' and len(clients[name].get_jobs().json['jobs']) == 1
    assert len({key for key, _ in first.values()}) == 3
    assert sum(len(c.posts) for c in clients.values()) == 3

    # New worker invocations reuse each original account/order; no extra POST.
    for name in clients:
        result = backfill.tick(config, root, account=name)
        assert result['init'] == first[name][0]
    assert sum(len(c.posts) for c in clients.values()) == 3

    # The 12 UTC live init is now due on b. History on the other accounts still
    # resumes the first receipts, submits remaining groups and publishes.
    clock[0] += dt.timedelta(hours=12)
    blocked = backfill.tick(config, root, account='b')
    assert blocked['status'] == 'waiting_for_realtime' and blocked['priority_inits'] == ['2026102712']
    trace.append(blocked)
    manifests = []
    for name in ('a', 'c', 'b'):
        client = clients[name]
        finish[name] = True
        for remote in client.remotes.values():
            remote.json['status'] = 'successful'
        if name == 'b':
            # Once b's existing ADS order has finished, live uses the slot first.
            live = worker.run_init(config, '20261027', 12, client=client)
            assert live['status'] == 'complete'
            trace.append(live)
        result = backfill.tick(config, root, account=name)
        trace.append({'account': name, **result})
        assert result['status'] == 'complete' and result['init'] == first[name][0]
        key = result['init']
        path = config.output_dir/'ec_cams'/key[:8]/(key[8:]+'z')/(key[:8]+'_'+key[8:]+'z_ec_cams.manifest.json')
        manifest = json.loads(path.read_text())
        assert len(manifest['files']) == 121
        assert sum(f['records_selected'] for f in manifest['files']) == 5732
        history_posts = [post for post in client.posts if post['date'] == client.posts[0]['date']]
        assert history_posts[0]['request_id'] == first[name][1]
        assert len(history_posts) == 3 and not client.get_jobs().json['jobs']
        manifests.append(str(path))
    assert backfill.active_week(root, plan) == plan['weeks'][0]  # 3/14 is not acceptance.
    seed.posts = [dict(account=name, **post) for name, client in clients.items() for post in client.posts]
    report('three-accounts', config, seed, trace, {
        'three_distinct_init_owners': first, 'maximum_active_orders_per_account': 1,
        'restart_additional_posts': 0, 'published_inits': 3, 'messages_per_init': 5732,
        'files_per_init': 121, 'full_week_not_prematurely_accepted': True, 'manifests': manifests,
        'other_accounts_completed_while_live_was_due': ['a', 'c'], 'live_preempted_shared_account': 'b'})


def test_due_live_retry_wins_but_expired_or_exhausted_recovery_does_not_stall_history(system, monkeypatch):
    config, clock, client, tmp = system
    config = config.model_copy(update={'history_accounts': ['mock-account'],
        'max_active_orders_per_account': 1, 'mars_count_retry_limit': 48,
        'mars_count_retry_delays_seconds': [1800]*48, 'mars_count_retry_window_seconds': 86400})
    client.failures = 1
    started = clock[0]
    result = worker.run_init(config, '20261027', 0)
    assert result['status'] == 'recovery_deferred' and len(client.posts) == 1
    root = tmp/'due-priority-queue'
    save(root/'plan.json', backfill.make_plan('20260101', '20260921', authorized_at=started.isoformat()))
    clock[0] = started+dt.timedelta(seconds=1799)
    assert not worker.due_recovery_inits(config, 0, clock[0])
    clock[0] += dt.timedelta(seconds=1)
    assert worker.due_recovery_inits(config, 0, clock[0]) == ['2026102700']
    assert backfill.tick(config, root, account='mock-account')['status'] == 'waiting_for_realtime'
    assert not list((root/'assignments').glob('*.json'))
    folder=config.output_dir/'ec_cams/20261027/00z/.cams'
    targets=[p for p in folder.rglob('state.json')
             if json.loads(p.read_text()).get('phase')=='retry_wait' and json.loads(p.read_text()).get('request_id')]
    assert targets
    saved={p:p.read_bytes() for p in targets}
    for p in targets:
        value=json.loads(p.read_text());value['retry_count']=48;save(p,value)
    assert not worker.due_recovery_inits(config,0,clock[0])
    for p,raw in saved.items():p.write_bytes(raw)
    clock[0]=started+dt.timedelta(seconds=86401)
    assert not worker.due_recovery_inits(config,0,clock[0])
    assert len(client.posts)==1
