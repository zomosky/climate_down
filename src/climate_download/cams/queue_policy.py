"""Pure-stdlib authorization shared with the independent host supervisor.

The same file is copied beside the host entry point; no upstream package import
is needed there. It overrides only runtime scheduling, never the signed plan.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path


def make_policy(root: Path, plan: dict, delays: list[int], *, authorized_at: str,
                retry_window_seconds: int | None = None, deleted_order_retry_limit: int = 0,
                continue_other_weeks_when_blocked: bool = False) -> dict:
    daily = retry_window_seconds is not None
    if (not isinstance(delays, list) or not 1 <= len(delays) <= (48 if daily else 3)
            or any(type(d) is not int or not 300 <= d <= 21600 for d in delays)
            or (daily and (type(retry_window_seconds) is not int or not 1800 <= retry_window_seconds <= 86400
                           or any(d != 1800 for d in delays) or sum(delays) > retry_window_seconds))
            or (not daily and any(a >= b for a, b in zip(delays, delays[1:])))
            or type(deleted_order_retry_limit) is not int or deleted_order_retry_limit not in (0,1)
            or (deleted_order_retry_limit and not daily)
            or type(continue_other_weeks_when_blocked) is not bool
            or not isinstance(authorized_at, str) or not authorized_at
            or dt.datetime.fromisoformat(authorized_at.replace('Z', '+00:00')).tzinfo is None):
        raise ValueError('An explicit dated and bounded staged recovery authorization is required')
    policy = {'schema_version': 2 if daily else 1, 'source': plan['source'], 'dataset': plan['dataset'],
            'plan_sha256': hashlib.sha256((root/'plan.json').read_bytes()).hexdigest(),
            'authorized_at': authorized_at, 'strategy': 'bounded_staged_mars_count_recovery',
            'retry_delays_seconds': list(delays), 'max_replacements_per_init': len(delays),
            'continue_other_weeks_when_deferred': True,
            'legacy_plan_operational_overrides': {'failed_request_resubmission': True,
                                                'acceptance_required_before_previous_week': False}}
    if daily:
        policy['retry_window_seconds'] = retry_window_seconds
    if deleted_order_retry_limit or continue_other_weeks_when_blocked:
        policy.update(schema_version=3, deleted_order_retry_limit=deleted_order_retry_limit,
                      continue_other_weeks_when_blocked=continue_other_weeks_when_blocked)
    return policy


def load_policy(root: Path, plan: dict) -> dict | None:
    path = root/'recovery_policy.json'
    if not path.is_file():
        return None
    data = json.loads(path.read_text())
    expected = make_policy(root, plan, data.get('retry_delays_seconds'), authorized_at=data.get('authorized_at'),
                           retry_window_seconds=data.get('retry_window_seconds'),
                           deleted_order_retry_limit=data.get('deleted_order_retry_limit',0),
                           continue_other_weeks_when_blocked=data.get('continue_other_weeks_when_blocked',False))
    if data != expected:
        raise ValueError('Weekly staged recovery authorization or signed-plan identity changed')
    return data
