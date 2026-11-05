"""Immutable historical init ownership; no ADS calls or changes to old evidence."""
from __future__ import annotations

import datetime as dt
import fcntl
import json
from pathlib import Path

from climate_download.cams.catalogue import SOURCE
from climate_download.cams.config import CamsConfig
from climate_download.cams.state import now, save, signature


def states(config: CamsConfig, key: str, *, include_archives: bool = False) -> list[dict]:
    folder = config.output_dir/SOURCE/key[:8]/(key[8:]+'z')/'.cams'
    paths = (list(folder.rglob('state.json')) if include_archives else
             list(folder.glob('*/state.json')) + list((folder/'hourly/parts').glob('*/state.json')))
    return [json.loads(p.read_text()) for p in paths]


def owner(config: CamsConfig, root: Path, plan: dict, key: str) -> str | None:
    if not any(key in week['inits'] for week in plan['weeks']):
        raise ValueError('Historical assignment is outside its authorized plan')
    saved = states(config, key, include_archives=True)
    original = {s['account'] for s in saved if s.get('account')}
    if len(original) > 1 or not original <= {a.name for a in config.accounts}:
        raise ValueError('Retained CAMS accounts disagree; preserve and inspect the original records')
    known = next(iter(original), None)
    folder = config.output_dir/SOURCE/key[:8]/(key[8:]+'z')
    path = root/'assignments'/(key+'.json')
    if not path.exists():
        if known is None and folder.exists() and any(p.is_file() for p in folder.rglob('*')):
            # Without a prior claim, orphan evidence belongs to the old lane.
            known = config.account_for(int(key[8:])).name
        return known
    assignment = json.loads(path.read_text())
    if (set(assignment) != {'schema_version', 'source', 'init', 'account', 'plan_signature', 'claimed_at'}
            or assignment['schema_version'] != 1 or assignment['source'] != SOURCE
            or assignment['init'] != key or assignment['account'] not in config.history_accounts
            or assignment['plan_signature'] != signature(plan)
            or dt.datetime.fromisoformat(assignment['claimed_at']).tzinfo is None
            or known is not None and known != assignment['account']):
        raise ValueError('Fixed CAMS historical assignment differs from retained evidence or plan')
    return assignment['account']


def claim(config: CamsConfig, root: Path, plan: dict, key: str, account: str) -> bool:
    if account not in config.history_accounts:
        raise ValueError('Account is not authorized for historical work')
    root.mkdir(parents=True, exist_ok=True)
    with (root/'assignment.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        existing = owner(config, root, plan, key)
        if existing is not None:
            return existing == account
        save(root/'assignments'/(key+'.json'), {
            'schema_version': 1, 'source': SOURCE, 'init': key, 'account': account,
            'plan_signature': signature(plan), 'claimed_at': now()})
        return True


def active_init(config: CamsConfig, plan: dict, jobs: list[dict]) -> str | None:
    """Locate one current receipt, never adopting an unknown or archived job ID."""
    if not jobs:
        return None
    if len(jobs) != 1 or not jobs[0].get('jobID'):
        raise ValueError('Account has multiple or unverifiable active ADS orders')
    request_id = jobs[0]['jobID']
    matches = [key for week in plan['weeks'] for key in week['inits']
               if any(s.get('request_id') == request_id for s in states(config, key))]
    if len(matches) != 1:
        raise ValueError('Active ADS order does not have one retained historical receipt')
    return matches[0]
