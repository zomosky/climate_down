"""Host entry for a finite weekly CAMS queue and saved full-week acceptance."""
from __future__ import annotations

import argparse
import datetime as dt
import fcntl
import hashlib
import importlib.util
import json
from pathlib import Path
import re
import subprocess


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def read(path):
    return json.loads(path.read_text())


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(8 << 20), b''):
            h.update(block)
    return h.hexdigest()


def save(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name('.'+path.name+'.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False)+'\n')
    tmp.replace(path)


def safe(value):
    value = re.sub(r'https?://[^\s\"\x27<>]+', '[URL_REDACTED]', value)
    return re.sub(r'[\w.+-]+@[\w.-]+', '[EMAIL_REDACTED]', value)


def checked_plan(op):
    deployment = read(op/'deployment.json')
    root = Path(deployment['host_queue_root'])
    assert sha(root/'plan.json') == deployment['plan_sha256'], 'Pinned weekly plan changed'
    plan = read(root/'plan.json')
    return deployment, root, plan


def download_tick(op, cycle):
    deployment, root, _ = checked_plan(op)
    command = ['docker', 'exec', '-e', 'PYTHONDONTWRITEBYTECODE=1', '-w', '/workspace/climate_down',
               'zhangmy-dev', '/workspace/climate_down/.venv/bin/python', '-m', 'climate_download.cams.backfill',
               '--config', 'config/jobs/cams_operational.yaml', '--root', deployment['container_queue_root'],
               '--plan-sha256', deployment['plan_sha256'], '--cycle', str(cycle)]
    started = now()
    result = subprocess.run(command, capture_output=True, text=True, timeout=1200)
    (op/f'worker-{cycle:02d}.log').write_text(safe(result.stdout+result.stderr))
    path = root/f'worker-{cycle:02d}.json'
    state = read(path) if path.is_file() else {'status': 'failed', 'reason': 'Container did not save its worker state'}
    save(op/f'run-{cycle:02d}.json', {'started_at': started, 'finished_at': now(),
                                    'returncode': result.returncode, 'worker': state})
    assert result.returncode in (0, 1) and state.get('status') != 'failed', 'Weekly worker failed; inspect its saved state'
    return state


def raw_ready(deployment, key):
    root = Path(deployment['host_data_root'])
    folder = root/'ec_cams'/key[:8]/(key[8:]+'z')
    manifest = folder/f'{key[:8]}_{key[8:]}z_ec_cams.manifest.json'
    if not manifest.is_file():
        return False
    value = read(manifest)
    return (value.get('completed_at') and not value.get('failures') and len(value.get('files', [])) == 121
            and sum(f['records_selected'] for f in value['files']) == 5732)


def ready(deployment, key):
    root = Path(deployment['host_data_root'])
    marker = root/'climate_data_storage/zarr/ec_cams'/f'{key[:8]}_{key[8:]}z_ec_cams.zarr/.zmetadata'
    return marker.is_file() and raw_ready(deployment, key)


def run_stage(deployment, root, week, key, stage):
    folder = root/'weeks'/week['id']/'acceptance'/key
    folder.mkdir(parents=True, exist_ok=True)
    container = 'zhangmy-dev' if stage == 'raw' else 'climate-watcher'
    python = '/workspace/climate_restorage/.venv/bin/python' if stage == 'raw' else '/workspace/climate_watcher/.venv/bin/python'
    inside = deployment['container_queue_root']
    script = root/'cams_accept_backfill.py'
    assert sha(script) == deployment['acceptance_sha256'], 'Pinned acceptance script changed'
    temp_root = '/tmp/cams-weekly-20261027/'+week['id']
    command = ['docker', 'exec', '-e', 'PYTHONDONTWRITEBYTECODE=1',
               '-e', 'CAMS_ACCEPT_ROOT='+inside+'/acceptance/'+week['id'],
               '-e', 'CAMS_EXPORT_ROOT='+temp_root, container, python, inside+'/cams_accept_backfill.py',
               stage, '--date', key[:8], '--cycle', str(int(key[8:]))]
    result = subprocess.run(command, capture_output=True, text=True, timeout=1200)
    (folder/(stage+'.log')).write_text(safe(result.stdout+result.stderr))
    assert result.returncode == 0, f'{stage} acceptance failed for {key}; inspect its retained log'
    case = key[:8]+'-'+key[8:]
    if stage == 'raw':
        save(folder/'raw_acceptance.json', read(root/'acceptance'/week['id']/case/'raw_acceptance.json'))
    else:
        result = subprocess.run(['docker', 'cp', f'{container}:{temp_root}/{case}/.', str(folder)], capture_output=True)
        assert result.returncode == 0, 'Copying accepted HTTP artifacts failed'


def accepted_case(folder, key):
    raw, http = (read(folder/(s+'_acceptance.json')) for s in ('raw', 'http'))
    for value in (raw, http):
        assert value.get('passed') is True and value['date'] == key[:8] and value['cycle'] == int(key[8:])
    assert raw['messages'] == 5732 and len(raw['variables']) == 52 and raw['zarr_exact']
    assert raw['raw_sha256_and_split_bytes_verified'] and raw['native_times_zero_shift']
    assert http['variable_count'] == 52 and http['native_samples'] == 121 and http['resampled_samples'] == 481
    for field in ('region_raw_exact', 'zero_shift', 'energy_independent_flux_passed', 'h0_missing_preserved',
                  'train_and_operation_same_init_values_equal', 'aod_not_differenced',
                  'sparse_native_missingness_preserved', 'no_unexpected_interior_missingness'):
        assert http[field], field
    assert len(http['exports']) == (7 if http['multi_source_checked'] else 5)
    if http['multi_source_checked']:
        assert http['multi_source_values_and_provenance_equal']
    exports = []
    for item in http['exports']:
        path = folder/item['file']
        assert path.is_file() and path.stat().st_size == item['bytes'] and sha(path) == item['sha256']
        assert item['netcdf_readback']
        exports.append({'file': item['file'], 'sha256': item['sha256'], 'bytes': item['bytes']})
    return {'passed': True, 'init': key, 'raw_sha256': sha(folder/'raw_acceptance.json'),
            'http_sha256': sha(folder/'http_acceptance.json'), 'exports': exports,
            'multi_source_checked': http['multi_source_checked']}


def accept_tick(op):
    deployment, root, plan = checked_plan(op)
    policy = None
    if (root/'recovery_policy.json').is_file():
        # Production has a pure-stdlib copy beside this host entry; local tests
        # use its canonical package source without importing upstream packages.
        module_path = Path(__file__).with_name('cams_queue_policy.py')
        if not module_path.is_file():
            module_path = Path(__file__).resolve().parents[1]/'src/climate_download/cams/queue_policy.py'
        spec = importlib.util.spec_from_file_location('cams_queue_policy', module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        policy = module.load_policy(root, plan)
    # Use the same signed-plan and assessment validation as the download worker.
    pending, accepted = [], 0
    for candidate in plan['weeks']:
        path = root/'weeks'/candidate['id']/'assessment.json'
        if not path.is_file():
            pending.append(candidate)
            continue
        receipt = read(path)
        assert receipt['passed'] is True and receipt['week'] == candidate
        assert receipt['plan_signature'] == hashlib.sha256(json.dumps(plan, sort_keys=True).encode()).hexdigest()
        assert [case['init'] for case in receipt['cases']] == candidate['inits']
        for case in receipt['cases']:
            assert case['passed'] is True
            for stage in ('raw', 'http'):
                proof = root/'weeks'/candidate['id']/'acceptance'/case['init']/(stage+'_acceptance.json')
                assert sha(proof) == case[stage+'_sha256'], 'Previously accepted weekly evidence changed'
        accepted += 1
    if not pending:
        result = {'phase': 'complete', 'checked_at': now(), 'accepted_weeks': len(plan['weeks']),
                  'accepted_inits': plan['inits_total']}
        save(op/'status.json', result)
        return result
    week = pending[0]
    if policy:
        week = next((candidate for candidate in pending
                     if all(ready(deployment, key) for key in candidate['inits'])), week)
    state = {'phase': 'downloading_week', 'checked_at': now(), 'week': week['id'],
             'expected_inits': len(week['inits']), 'total_weeks': len(plan['weeks']),
             'accepted_weeks': accepted, 'target_start': plan['start'],
             'unaccepted_weeks': [candidate['id'] for candidate in pending]}
    missing = [key for key in week['inits'] if not ready(deployment, key)]
    if missing:
        state.update(missing_inits=missing, ready_inits=len(week['inits'])-len(missing))
        if all(raw_ready(deployment, key) for key in week['inits']):
            state['phase'] = 'waiting_for_scheduled_restore'
        save(op/'status.json', state)
        return state
    state.update(phase='accepting_week', ready_inits=len(week['inits']))
    save(op/'status.json', state)
    # One complete init per tick bounds host work and keeps restart/resume cheap.
    for key in week['inits']:
        folder = root/'weeks'/week['id']/'acceptance'/key
        for stage in ('raw', 'http'):
            path = folder/(stage+'_acceptance.json')
            if not path.is_file():
                state.update(phase='accepting_'+stage, active_init=key)
                save(op/'status.json', state)
                run_stage(deployment, root, week, key, stage)
        accepted_case(folder, key)
        if not (folder/'case.json').is_file():
            save(folder/'case.json', {'passed': True, 'init': key, 'accepted_at': now()})
            return {**state, 'accepted_init': key}
    cases = [accepted_case(root/'weeks'/week['id']/'acceptance'/key, key) for key in week['inits']]
    # Signature is canonical JSON, identical to climate_download.cams.state.signature.
    canonical = json.dumps(plan, sort_keys=True).encode()
    receipt = {'passed': True, 'completed_at': now(), 'week': week,
               'plan_signature': hashlib.sha256(canonical).hexdigest(), 'cases': cases,
               'messages': len(cases)*5732, 'variables': 52}
    if policy:
        receipt['runtime_recovery_policy_sha256'] = sha(root/'recovery_policy.json')
    save(root/'weeks'/week['id']/'assessment.json', receipt)
    state.update(phase='week_accepted', accepted_weeks=state['accepted_weeks']+1)
    save(op/'status.json', state)
    return state


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--operation-dir', type=Path, required=True)
    parser.add_argument('--cycle', choices=(0, 12), type=int)
    parser.add_argument('--accept', action='store_true')
    args = parser.parse_args(argv)
    if args.accept == (args.cycle is not None):
        parser.error('Choose either --cycle or --accept')
    op = args.operation_dir
    op.mkdir(parents=True, exist_ok=True)
    kind = 'accept' if args.accept else f'cycle-{args.cycle:02d}'
    with (op/(kind+'.lock')).open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return 0
        failure = op/(kind+'.failure.json')
        if failure.is_file():
            print(json.dumps({'status': 'needs_inspection', 'failure_evidence': str(failure)}))
            return 1
        try:
            result = accept_tick(op) if args.accept else download_tick(op, args.cycle)
        except Exception as error:
            save(failure, {'checked_at': now(), 'error': safe(str(error)), 'type': type(error).__name__})
            return 1
        print(json.dumps(result, ensure_ascii=False))
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
