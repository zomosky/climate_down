"""The host gate checks actual complete cases and export bytes before advancing."""
import importlib.util
from pathlib import Path

import pytest

from climate_download.cams import backfill
from climate_download.cams.state import digest, save

spec = importlib.util.spec_from_file_location('cams_backfill_host', Path(__file__).parents[1]/'deploy/cams_backfill.py')
host = importlib.util.module_from_spec(spec)
spec.loader.exec_module(host)


def evidence(folder, key):
    raw = {'passed': True, 'date': key[:8], 'cycle': int(key[8:]), 'messages': 5732,
           'variables': [{}]*52, 'zarr_exact': True, 'raw_sha256_and_split_bytes_verified': True,
           'native_times_zero_shift': True}
    http = {field: True for field in ('passed', 'region_raw_exact', 'zero_shift',
            'energy_independent_flux_passed', 'h0_missing_preserved',
            'train_and_operation_same_init_values_equal', 'aod_not_differenced',
            'sparse_native_missingness_preserved', 'no_unexpected_interior_missingness')}
    http.update(date=key[:8], cycle=int(key[8:]), variable_count=52, native_samples=121,
                resampled_samples=481, multi_source_checked=False,
                multi_source_values_and_provenance_equal=None, exports=[])
    folder.mkdir(parents=True, exist_ok=True)
    for n in range(5):
        path = folder/f'export{n}.nc'
        path.write_bytes(b'netcdf readback fixture')
        http['exports'].append({'file': path.name, 'bytes': path.stat().st_size,
                                'sha256': digest(path), 'netcdf_readback': True})
    save(folder/'raw_acceptance.json', raw)
    save(folder/'http_acceptance.json', http)
    return raw, http


@pytest.mark.parametrize('damage', ['variable_count', 'sparse', 'export_bytes', 'wrong_init', 'messages'])
def test_incomplete_or_corrupt_case_is_rejected(tmp_path, damage):
    key = '2026091500'
    raw, http = evidence(tmp_path, key)
    if damage == 'variable_count': http['variable_count'] = 51
    elif damage == 'sparse': http['sparse_native_missingness_preserved'] = False
    elif damage == 'export_bytes': (tmp_path/'export0.nc').write_bytes(b'changed bytes')
    elif damage == 'wrong_init': http['cycle'] = 12
    elif damage == 'messages': raw['messages'] = 5731
    save(tmp_path/'raw_acceptance.json', raw)
    save(tmp_path/'http_acceptance.json', http)
    with pytest.raises(AssertionError):
        host.accepted_case(tmp_path, key)


def test_cams_case_can_pass_without_inventing_an_ifs_product(tmp_path):
    evidence(tmp_path, '2026091500')
    case = host.accepted_case(tmp_path, '2026091500')
    assert case['passed'] and case['multi_source_checked'] is False
    assert len(case['exports']) == 5


def test_host_full_week_gate_agrees_with_download_worker_signature(tmp_path, monkeypatch):
    root, op = tmp_path/'data', tmp_path/'op'
    plan = backfill.make_plan('20260908', '20260921', authorized_at='2026-10-27T00:00:00Z')
    save(root/'plan.json', plan)
    save(op/'deployment.json', {'host_queue_root': str(root), 'plan_sha256': digest(root/'plan.json')})
    week = plan['weeks'][0]
    for key in week['inits']:
        folder = root/'weeks'/week['id']/'acceptance'/key
        evidence(folder, key)
        save(folder/'case.json', {'passed': True})
    monkeypatch.setattr(host, 'ready', lambda *a: True)
    result = host.accept_tick(op)
    assert result['phase'] == 'week_accepted'
    assert backfill.active_week(root, plan) == plan['weeks'][1]


def test_host_never_starts_full_reads_until_whole_week_is_ingested(tmp_path, monkeypatch):
    root, op = tmp_path/'data', tmp_path/'op'
    plan = backfill.make_plan('20260915', '20260921', authorized_at='2026-10-27T00:00:00Z')
    save(root/'plan.json', plan)
    save(op/'deployment.json', {'host_queue_root': str(root), 'plan_sha256': digest(root/'plan.json')})
    monkeypatch.setattr(host, 'ready', lambda deployment, key: key != '2026092112')
    monkeypatch.setattr(host, 'raw_ready', lambda *a: True)
    monkeypatch.setattr(host, 'run_stage', lambda *a: pytest.fail('week has a missing init'))
    state = host.accept_tick(op)
    assert state['phase'] == 'waiting_for_scheduled_restore' and state['ready_inits'] == 13
    assert not (root/'weeks'/plan['weeks'][0]['id']/'assessment.json').exists()
