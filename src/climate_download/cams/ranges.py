"""Bounded same-object range transfers; only verified contiguous bytes are appended."""
from __future__ import annotations

import concurrent.futures
import hashlib
import json
from pathlib import Path
import time

from climate_download.cams.state import digest, now, save


class RangeIdentityError(ValueError):
    """Never append bytes from a different object or range."""


def _open_range(session, url, start, end, expected, etag, timeout):
    response = session.get(url, headers={'Range': f'bytes={start}-{end}', 'Accept-Encoding': 'identity'},
                           stream=True, timeout=timeout)
    try:
        response.raise_for_status()
    except Exception:
        response.close()
        raise
    if (response.status_code != 206 or response.headers.get('Content-Range') != f'bytes {start}-{end}/{expected}'
            or int(response.headers.get('Content-Length', -1)) != end-start+1
            or response.headers.get('Content-Encoding', 'identity') != 'identity'
            or (etag is not None and response.headers.get('ETag') != etag)):
        response.close()
        raise RangeIdentityError('ADS object/range identity changed; existing raw bytes retained')
    return response


def _commit(part, root, spans, identity):
    current = part.stat().st_size if part.exists() else 0
    for start, end in spans:
        chunk, receipt = root/f'{start}-{end}.part', root/f'{start}-{end}.json'
        if not receipt.is_file():
            if current > end:
                continue
            break
        proof = json.loads(receipt.read_text())
        if (proof.get('start'), proof.get('end'), proof.get('bytes')) != (start, end, end-start+1):
            raise RangeIdentityError('Saved range receipt differs')
        if any(proof.get(k) != identity[k] for k in ('request_id', 'etag')):
            raise RangeIdentityError('Saved range belongs to a different object')
        if current > end:
            with part.open('rb') as stream:
                stream.seek(start)
                if hashlib.sha256(stream.read(end-start+1)).hexdigest() != proof['sha256']:
                    raise RangeIdentityError('Previously appended range bytes changed')
            if chunk.exists():
                if digest(chunk) != proof['sha256']:
                    raise RangeIdentityError('Previously appended range cache changed')
                chunk.unlink()
            continue
        if current < start or not chunk.is_file():
            break
        if chunk.stat().st_size != end-start+1 or digest(chunk) != proof['sha256']:
            raise RangeIdentityError('Completed range checksum changed')
        overlap = current-start
        if overlap:
            with part.open('rb') as target, chunk.open('rb') as source:
                target.seek(start)
                if target.read(overlap) != source.read(overlap):
                    raise RangeIdentityError('Interrupted range append differs from original bytes')
        with part.open('ab') as target, chunk.open('rb') as source:
            source.seek(overlap)
            for block in iter(lambda: source.read(1 << 20), b''):
                target.write(block)
            target.flush()
            __import__('os').fsync(target.fileno())
        current = part.stat().st_size
        if current != end+1:
            raise RangeIdentityError('Unexpected range append size')
        with part.open('rb') as target:
            target.seek(start)
            if hashlib.sha256(target.read(end-start+1)).hexdigest() != proof['sha256']:
                raise RangeIdentityError('Appended range checksum mismatch')
        chunk.unlink()
    return current


def transfer_ranges(url: str, part: Path, expected: int, *, request_id: str, connections: int,
                    seconds: int, chunk_bytes: int = 4 << 20) -> tuple[dict, bool]:
    import requests

    if not 1 <= connections <= 4 or chunk_bytes <= 0 or seconds <= 0 or expected <= 0:
        raise ValueError('Invalid bounded range transfer configuration')
    before = part.stat().st_size if part.exists() else 0
    if before > expected:
        raise RangeIdentityError('Partial exceeds pinned object length')
    if before == expected:
        return {'expected_bytes': expected, 'partial_bytes': before, 'already_transferred': True}, True
    began = time.monotonic()
    deadline = began+seconds
    with requests.Session() as session:
        session.trust_env = False
        with _open_range(session, url, 0, 0, expected, None, (10, min(20, seconds))) as response:
            etag = response.headers.get('ETag')
            if not etag or etag.startswith('W/'):
                raise RangeIdentityError('A strong object identity is required for parallel ranges')
            if len(next(response.iter_content(1), b'')) != 1:
                raise RangeIdentityError('Missing range identity probe byte')
    root = part.with_name(part.name+'.ranges')
    root.mkdir(parents=True, exist_ok=True)
    identity_path = root/'identity.json'
    identity = {'request_id': request_id, 'expected_bytes': expected, 'etag': etag}
    if identity_path.is_file():
        saved = json.loads(identity_path.read_text())
        if any(saved.get(k) != v for k, v in identity.items()):
            raise RangeIdentityError('Pinned range object changed; do not reset its partial')
        base = saved['base_offset']
        block_size = saved['chunk_bytes']
        if not isinstance(base, int) or not 0 <= base <= before or not isinstance(block_size, int) or block_size <= 0:
            raise RangeIdentityError('Saved range layout changed')
    else:
        base, block_size = before, chunk_bytes
        save(identity_path, {**identity, 'base_offset': base, 'chunk_bytes': block_size, 'created_at': now()})
    spans = [(start, min(start+block_size-1, expected-1)) for start in range(base, expected, block_size)]
    fetched, errors = [], []

    def fetch(span):
        start, end = span
        if end < before or time.monotonic() >= deadline:
            return 0, None
        path, proof_path = root/f'{start}-{end}.part', root/f'{start}-{end}.json'
        offset = path.stat().st_size if path.exists() else 0
        if offset > end-start+1:
            raise RangeIdentityError('Range cache exceeds its pinned length')
        if proof_path.is_file():
            proof = json.loads(proof_path.read_text())
            if (any(proof.get(k) != identity[k] for k in ('request_id', 'etag'))
                    or not path.is_file() or path.stat().st_size != end-start+1 or digest(path) != proof['sha256']):
                raise RangeIdentityError('Range cache checksum changed')
            return 0, None
        count = 0
        try:
            if offset < end-start+1:
                with requests.Session() as session:
                    session.trust_env = False
                    timeout = (10, max(1, min(10, deadline-time.monotonic())))
                    with _open_range(session, url, start+offset, end, expected, etag, timeout) as response, path.open('ab') as stream:
                        for block in response.iter_content(16384):
                            if not block:
                                continue
                            if offset+count+len(block) > end-start+1:
                                raise RangeIdentityError('Server returned bytes outside the requested range')
                            stream.write(block)
                            count += len(block)
                            if time.monotonic() >= deadline:
                                break
                        stream.flush()
                        __import__('os').fsync(stream.fileno())
            if path.stat().st_size == end-start+1:
                save(proof_path, {'start': start, 'end': end, 'bytes': end-start+1,
                                  'sha256': digest(path), 'request_id': request_id, 'etag': etag})
            return count, None
        except RangeIdentityError:
            raise
        except requests.RequestException as error:
            # URLs and request headers must never enter a saved transport error.
            return count, type(error).__name__

    with concurrent.futures.ThreadPoolExecutor(max_workers=connections) as pool:
        futures = [pool.submit(fetch, span) for span in spans]
        for future in concurrent.futures.as_completed(futures):
            amount, error = future.result()
            fetched.append(amount)
            if error:
                errors.append(error)
    after = _commit(part, root, spans, identity)
    if not before <= after <= expected:
        raise RangeIdentityError('Unsafe parallel partial size change')
    elapsed = round(time.monotonic()-began, 3)
    receipt = {'at': now(), 'mode': 'bounded_ranges', 'connections': connections,
               'expected_bytes': expected, 'partial_bytes': after, 'bytes_before': before,
               'elapsed_seconds': elapsed, 'returncode': 0 if after == expected else 28,
               'http': {'http_code': 206, 'size_download': sum(fetched),
                        'speed_download': round(sum(fetched)/max(elapsed, .001)), 'time_total': elapsed},
               'cached_range_bytes': sum(p.stat().st_size for p in root.glob('*.part')),
               'transport_errors': dict(__import__('collections').Counter(errors))}
    return receipt, after == expected
