"""NOAA GEFS 00z selected-member sub-link, with identity-safe resumable output.

Multiple member/a+b objects form one step. The ready manifest is published only
when every requested member/step has passed GRIB-header validation. No ECDS queue.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import tempfile

from climate_download.sources._http import build_client, request_with_retry

HOST = 'https://noaa-gefs-pds.s3.amazonaws.com'
STEPS = tuple(range(0, 241, 3)) + tuple(range(246, 841, 6))
# (upstream parameter, upstream level, output variable, GRIB stepType)
FIELDS = {
    'a': [('TMP', '2 m above ground', 't2m', 'instant'),
          ('RH', '2 m above ground', 'r2', 'instant'),
          ('UGRD', '10 m above ground', 'u10', 'instant'),
          ('VGRD', '10 m above ground', 'v10', 'instant'),
          ('PRES', 'surface', 'sp', 'instant'),
          ('PRMSL', 'mean sea level', 'prmsl', 'instant'),
          ('TMAX', '2 m above ground', 'tmax2m_max', 'max'),
          ('TMIN', '2 m above ground', 'tmin2m_min', 'min'),
          ('APCP', 'surface', 'tp_accum', 'accum'),
          ('DSWRF', 'surface', 'sdswrf_avg', 'avg'),
          ('DLWRF', 'surface', 'sdlwrf_avg', 'avg'),
          ('TCDC', 'entire atmosphere', 'tcc_avg', 'avg')],
    'b': [('DPT', '2 m above ground', 'd2m', 'instant'),
          ('UGRD', '100 m above ground', 'u100', 'instant'),
          ('VGRD', '100 m above ground', 'v100', 'instant')],
}

# Header identities are validated independently of the mutable remote .idx text.
GRIB_FIELDS = {
    't2m': (167, 'heightAboveGround', 2), 'd2m': (168, 'heightAboveGround', 2),
    'r2': (260242, 'heightAboveGround', 2), 'u10': (165, 'heightAboveGround', 10),
    'v10': (166, 'heightAboveGround', 10), 'u100': (131, 'heightAboveGround', 100),
    'v100': (132, 'heightAboveGround', 100), 'sp': (134, 'surface', 0),
    'prmsl': (260074, 'meanSea', 0), 'tmax2m_max': (3015, 'heightAboveGround', 2),
    'tmin2m_min': (3016, 'heightAboveGround', 2), 'tp_accum': (228228, 'surface', 0),
    'sdswrf_avg': (260087, 'surface', 0), 'sdlwrf_avg': (260097, 'surface', 0),
    'tcc_avg': (228164, 'atmosphere', 0),
}


def member_tag(member: int) -> str:
    return 'c00' if member == 0 else f'p{member:02d}'


def select_members(count: int | None, text: str | None) -> tuple[int, ...]:
    if count is not None and text is not None:
        raise ValueError('member-count and members are mutually exclusive')
    if text is None:
        count = 5 if count is None else count
        if not 1 <= count <= 31:
            raise ValueError('member-count must be 1..31 (includes control)')
        return tuple(range(count))
    result = []
    for value in text.lower().split(','):
        value = value.strip()
        if value == 'c00':
            member = 0
        elif value.startswith('p') and value[1:].isdigit() and 1 <= int(value[1:]) <= 30:
            member = int(value[1:])
        elif value.isdigit():
            member = int(value)
        else:
            raise ValueError('members must use c00,p01..p30 or 0..30')
        result.append(member)
    if not result or len(set(result)) != len(result) or any(not 0 <= m <= 30 for m in result):
        raise ValueError('members must be unique in 0..30')
    return tuple(sorted(result))


def sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def atomic_bytes(path: Path, data: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix='.' + path.name, dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def save(path, value):
    atomic_bytes(path, json.dumps(value, indent=2).encode())


def now():
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Plan:
    date: str
    members: tuple[int, ...]
    steps: tuple[int, ...]
    output_dir: Path
    base_url: str = HOST

    def __post_init__(self):
        if datetime.strptime(self.date, '%Y%m%d').strftime('%Y%m%d') != self.date:
            raise ValueError('date must be YYYYMMDD')
        if not self.members or len(set(self.members)) != len(self.members) or any(type(m) is not int or m not in range(31) for m in self.members):
            raise ValueError('members must be unique integers in 0..30')
        if not self.steps or len(set(self.steps)) != len(self.steps) or any(type(s) is not int or s not in STEPS for s in self.steps):
            raise ValueError('steps must use native 0..840h endpoints')

    @property
    def folder(self):
        return self.output_dir.resolve() / 'gefs-ens' / self.date / '00z'

    @property
    def identity(self):
        return dict(source='gefs-ens', date=self.date, cycle=0,
                    members=list(self.members), steps=list(self.steps),
                    variables=[f[2] for fields in FIELDS.values() for f in fields])

    def url(self, member, step, part):
        return (f'{self.base_url.rstrip(chr(47))}/gefs.{self.date}/00/atmos/pgrb2{part}p5/'
                f'ge{member_tag(member)}.t00z.pgrb2{part}.0p50.f{step:03d}')


def validate_grib(data: bytes, plan: Plan, member: int, step: int, expected: list[tuple]):
    # Optional dependency: imported only by an actual download/resume, not dry-run.
    import eccodes as ec
    pos = 0
    for parameter, level, name, statistic in expected:
        if data[pos:pos + 4] != b'GRIB' or data[pos + 7:pos + 8] != b'\x02':
            raise ValueError(f'invalid GRIB2 framing for {name}')
        size = int.from_bytes(data[pos + 8:pos + 16], 'big')
        blob = data[pos:pos + size]
        if size < 20 or len(blob) != size or blob[-4:] != b'7777':
            raise ValueError(f'truncated GRIB2 for {name}')
        g = ec.codes_new_from_message(blob)
        try:
            expected_headers = {'dataDate': int(plan.date), 'dataTime': 0,
                'perturbationNumber': member, 'Ni': 720, 'Nj': 361, 'endStep': step,
                'stepType': statistic, 'dataType': 'cf' if member == 0 else 'pf',
                'typeOfEnsembleForecast': 1 if member == 0 else 3,
                'numberOfForecastsInEnsemble': 30,
                'iDirectionIncrementInDegrees': 0.5, 'jDirectionIncrementInDegrees': 0.5}
            expected_headers.update(zip(('paramId', 'typeOfLevel', 'level'), GRIB_FIELDS[name]))
            expected_headers.update(centre='kwbc', stepUnits=1, gridType='regular_ll')
            for key, value in expected_headers.items():
                if ec.codes_get(g, key) != value:
                    raise ValueError(f'GEFS {name}: {key} does not match requested product')
            start = ec.codes_get(g, 'startStep')
            if not 0 <= start <= step or (statistic != 'instant' and start == step):
                raise ValueError(f'invalid GEFS statistic window for {name}')
        finally:
            ec.codes_release(g)
        pos += size
    if pos != len(data):
        raise ValueError('unexpected extra GEFS messages')


def expected_fields(step):
    return [f for fields in FIELDS.values() for f in fields if step or f[3] == 'instant']


def download_member(plan: Plan, member: int, step: int):
    folder = plan.folder / '.members' / member_tag(member)
    raw = folder / f'f{step:03d}.grib2'
    state = raw.with_suffix('.json')
    expected = expected_fields(step)
    if raw.exists() and state.exists():
        old = json.loads(state.read_text())
        data = raw.read_bytes()
        if old.get('sha256') == sha(data) and old.get('member') == member and old.get('step') == step:
            validate_grib(data, plan, member, step, expected)
            return old
    chunks, provenance = [], []
    with build_client(timeout=120) as client:
        for part, fields in FIELDS.items():
            url = plan.url(member, step, part)
            head = request_with_retry(client, 'HEAD', url)
            head.raise_for_status()
            size, etag = int(head.headers['Content-Length']), head.headers.get('ETag')
            idx = request_with_retry(client, 'GET', url + '.idx')
            idx.raise_for_status()
            rows = [line.split(':') for line in idx.text.splitlines() if line.strip()]
            selected = []
            for field in fields:
                parameter, level, name, statistic = field
                matches = [i for i, row in enumerate(rows) if row[3:5] == [parameter, level]]
                if step == 0 and statistic != 'instant':
                    if matches:
                        raise ValueError(f'unexpected f000 statistic {name}')
                    continue
                if len(matches) != 1:
                    raise ValueError(f'GEFS index field not unique: {name}')
                i = matches[0]
                if rows[i][2] != f'd={plan.date}00':
                    raise ValueError('GEFS index init mismatch')
                lo = int(rows[i][1]); end = int(rows[i + 1][1]) if i + 1 < len(rows) else size
                if not 0 <= lo < end <= size:
                    raise ValueError('invalid GEFS index offset')
                selected.append((lo, end, field))
            ranges = []
            for lo, end, _ in sorted(selected):
                if ranges and lo == ranges[-1][1]:
                    ranges[-1][1] = end
                else:
                    ranges.append([lo, end])
            messages = {}
            for lo, end in ranges:
                headers = {'Range': f'bytes={lo}-{end - 1}'}
                if etag:
                    headers['If-Match'] = etag
                response = request_with_retry(client, 'GET', url, headers=headers)
                response.raise_for_status()
                if response.status_code != 206 or response.headers.get('Content-Range') != f'bytes {lo}-{end - 1}/{size}' or len(response.content) != end - lo:
                    raise ValueError('server did not return the exact requested byte range')
                for start, stop, field in selected:
                    if lo <= start < stop <= end:
                        messages[field[2]] = response.content[start-lo:stop-lo]
            # Decode in declared field order, independent of upstream message order.
            chunks.extend(messages[field[2]] for _, _, field in selected)
            provenance.append(dict(url=url, etag=etag, index_sha256=sha(idx.content),
                                   ranges=ranges))
    data = b''.join(chunks)
    validate_grib(data, plan, member, step, expected)
    atomic_bytes(raw, data)
    result = dict(member=member, step=step, path=str(raw), size_bytes=len(data),
                  sha256=sha(data), records=len(expected), sources=provenance)
    save(state, result)
    return result


def run(plan: Plan, workers=4):
    if not 1 <= workers <= 8:
        raise ValueError('workers must be 1..8')
    plan.folder.mkdir(parents=True, exist_ok=True)
    with (plan.folder / '.download.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        identity_path = plan.folder / '.gefs-request.json'
        manifest_path = plan.folder / f'{plan.date}_00z_gefs-ens.manifest.json'
        if identity_path.exists():
            if json.loads(identity_path.read_text()) != plan.identity:
                raise ValueError('existing init has a different member/step selection; use another output-dir')
        elif manifest_path.exists() or any(plan.folder.glob('*.grib2')):
            raise ValueError('existing unowned output; use another output-dir')
        save(identity_path, plan.identity)
        results, failures = [], []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(download_member, plan, m, s): (m, s) for m in plan.members for s in plan.steps}
            for future in as_completed(futures):
                try:
                    results.append(future.result())
                except Exception as exc:
                    m, s = futures[future]
                    failures.append(dict(member=m, step=s, error=str(exc)))
                save(plan.folder / '.download-status.json', dict(updated_at=now(), completed=len(results),
                     expected=len(futures), failures=failures))
        if failures:
            # A formerly complete manifest must not advertise corrupt/changed raw data.
            manifest_path.unlink(missing_ok=True)
            raise RuntimeError(f'{len(failures)} member/steps failed; no ready manifest; rerun to resume')
        files = []
        for step in sorted(plan.steps):
            rows = sorted((r for r in results if r['step'] == step), key=lambda r: r['member'])
            if [r['member'] for r in rows] != sorted(plan.members):
                raise ValueError('incomplete member set')
            payload = []
            for row in rows:
                data = Path(row['path']).read_bytes()
                if sha(data) != row['sha256']:
                    raise ValueError('member file changed during assembly')
                payload.append(data)
            data = b''.join(payload)
            path = plan.folder / f'f{step:03d}.subset.grib2'
            digest = sha(data)
            if not path.exists() or sha(path.read_bytes()) != digest:
                # Withdraw the old ready signal before mutating any published file.
                # Unchanged resumes retain its mtime; interrupted repairs stay unready.
                manifest_path.unlink(missing_ok=True)
                atomic_bytes(path, data)
            files.append(dict(step_hours=step, path=str(path), size_bytes=len(data), sha256=digest,
                              records_selected=sum(r['records'] for r in rows)))
        manifest = dict(schema_version=1, source=dict(name='gefs-ens', provider='NOAA', members=list(plan.members), grid_degrees=0.5),
            date=plan.date, cycle=0, init_time=datetime.strptime(plan.date, '%Y%m%d').replace(tzinfo=timezone.utc).isoformat(),
            completed_at=now(), variables=[dict(name=n) for n in plan.identity['variables']], files=files, failures=[])
        if manifest_path.exists():
            old = json.loads(manifest_path.read_text())
            if {k: v for k, v in old.items() if k != 'completed_at'} == {k: v for k, v in manifest.items() if k != 'completed_at'}:
                return manifest_path
        save(manifest_path, manifest)
        return manifest_path


def add_parser(sub):
    p = sub.add_parser('gefs-ens', aliases=['gefs-s2s'], help='GEFS 00z selected members, 35 days; anonymous NOAA download')
    p.add_argument('--date', required=True, help='YYYYMMDD, 00 UTC only')
    g = p.add_mutually_exclusive_group()
    g.add_argument('--member-count', type=int, help='1..31 including control; default 5')
    g.add_argument('--members', help='c00,p01,p02 or 0,1,2')
    p.add_argument('--steps', default='all', help='all or comma-separated native hours')
    p.add_argument('--output-dir', type=Path, default=Path('output'))
    p.add_argument('--workers', type=int, default=4)
    p.add_argument('--base-url', default=HOST, help='NOAA HTTP root; S3 archive or NOMADS recent data')
    p.add_argument('--dry-run', action='store_true')
    p.set_defaults(func=main)


def main(args):
    members = select_members(args.member_count, args.members)
    steps = STEPS if args.steps == 'all' else tuple(sorted(int(v) for v in args.steps.split(',')))
    plan = Plan(args.date, members, steps, args.output_dir, args.base_url)
    if args.dry_run:
        print(json.dumps({**plan.identity, 'output': str(plan.folder), 'expected_messages': len(members) * sum(len(expected_fields(s)) for s in steps)}, indent=2))
    else:
        print(json.dumps({'manifest': str(run(plan, args.workers)), 'status': 'complete'}))
    return 0
