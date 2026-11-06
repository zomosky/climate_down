from pathlib import Path
import json
import httpx
import pytest
from climate_download import gefs_s2s as g
from climate_download.cli import build_parser


def test_plan_and_cli():
    args = build_parser().parse_args(['gefs-s2s', '--date', '20261029', '--member-count', '5', '--dry-run'])
    assert g.select_members(args.member_count, args.members) == (0, 1, 2, 3, 4)
    assert len(g.STEPS) == 181 and g.STEPS[-1] == 840
    assert len(g.expected_fields(0)) == 9 and len(g.expected_fields(3)) == 15
    assert g.select_members(None, 'p04,c00,p02') == (0, 2, 4)


@pytest.mark.parametrize('count,text', [(0, None), (32, None), (2, 'c00'), (None, 'c00,0'), (None, 'p00'), (None, 'p31')])
def test_invalid_members(count, text):
    with pytest.raises(ValueError):
        g.select_members(count, text)


def test_plan_rejects_non_native_steps(tmp_path):
    with pytest.raises(ValueError):
        g.Plan('20261029', (0,), (241,), tmp_path)


def fake_member(plan, member, step):
    path = plan.folder / '.members' / str(member) / f'{step}.grib2'
    data = f'GRIB member={member} step={step} 7777'.encode()
    g.atomic_bytes(path, data)
    return dict(member=member, step=step, path=str(path), sha256=g.sha(data), size_bytes=len(data), records=len(g.expected_fields(step)))


def test_publish_resume_and_identity_guard(tmp_path, monkeypatch):
    monkeypatch.setattr(g, 'download_member', fake_member)
    plan = g.Plan('20261029', (0, 1), (0, 3), tmp_path)
    path = g.run(plan, 2)
    old = path.stat().st_mtime_ns
    value = json.loads(path.read_text())
    assert value['source']['members'] == [0, 1]
    assert [f['records_selected'] for f in value['files']] == [18, 30]
    assert g.run(plan, 2).stat().st_mtime_ns == old
    with pytest.raises(ValueError, match='different member/step'):
        g.run(g.Plan(plan.date, (0,), plan.steps, tmp_path))
    assert path.stat().st_mtime_ns == old


def test_one_failed_member_never_publishes(tmp_path, monkeypatch):
    def fail(plan, member, step):
        if member == 1:
            raise RuntimeError('network failed')
        return fake_member(plan, member, step)
    monkeypatch.setattr(g, 'download_member', fail)
    plan = g.Plan('20261029', (0, 1), (0, 3), tmp_path)
    with pytest.raises(RuntimeError, match='no ready manifest'):
        g.run(plan)
    assert not list(plan.folder.glob('*.manifest.json'))
    assert json.loads((plan.folder / '.download-status.json').read_text())['completed'] == 2


def test_unowned_output_refused(tmp_path):
    plan = g.Plan('20261029', (0,), (0,), tmp_path)
    plan.folder.mkdir(parents=True)
    (plan.folder / 'f000.subset.grib2').write_bytes(b'old')
    with pytest.raises(ValueError, match='unowned'):
        g.run(plan)


def test_ranges_coalesced_and_server_ignoring_range_rejected(tmp_path, monkeypatch, respx_mock):
    plan = g.Plan('20261029', (0,), (0,), tmp_path)
    monkeypatch.setattr(g, 'validate_grib', lambda *args: None)
    gets = []
    for part, fields in g.FIELDS.items():
        fields = [f for f in fields if f[3] == 'instant']
        payload = b''.join(f'{f[2]:<8}'.encode() for f in fields)
        index = '\n'.join(f'{i+1}:{i*8}:d=2026102900:{f[0]}:{f[1]}:anl:' for i, f in enumerate(fields))
        url = plan.url(0, 0, part)
        respx_mock.head(url).respond(200, headers={'Content-Length': str(len(payload)), 'ETag': '"v1"'})
        respx_mock.get(url + '.idx').respond(200, text=index)
        def reply(request, payload=payload):
            gets.append(request)
            assert request.headers['If-Match'] == '"v1"'
            return httpx.Response(206, content=payload, headers={'Content-Range': f'bytes 0-{len(payload)-1}/{len(payload)}'})
        respx_mock.get(url).mock(side_effect=reply)
    first = g.download_member(plan, 0, 0)
    assert len(gets) == 2
    assert g.download_member(plan, 0, 0) == first
    assert len(gets) == 2
    Path(first['path']).write_bytes(b'corrupt')
    respx_mock.get(plan.url(0, 0, 'a')).respond(200, content=b'whole-object')
    with pytest.raises(ValueError, match='exact requested byte range'):
        g.download_member(plan, 0, 0)


def test_interrupted_published_repair_withdraws_manifest(tmp_path, monkeypatch):
    monkeypatch.setattr(g, 'download_member', fake_member)
    plan = g.Plan('20261029', (0,), (0, 3), tmp_path)
    manifest = g.run(plan)
    # Both published files need repair, then the second atomic write fails.
    for f in plan.folder.glob('*.subset.grib2'):
        f.write_bytes(b'broken')
    original = g.atomic_bytes
    def fail_second(path, data):
        if path.name == 'f003.subset.grib2':
            raise OSError('disk full')
        original(path, data)
    monkeypatch.setattr(g, 'atomic_bytes', fail_second)
    with pytest.raises(OSError, match='disk full'):
        g.run(plan)
    assert not manifest.exists()
    monkeypatch.setattr(g, 'atomic_bytes', original)
    assert g.run(plan).exists()
