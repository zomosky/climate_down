"""A dated, request-scoped activation for failures predating a new retry rule."""
from __future__ import annotations

import datetime as dt
import json

from climate_download.cams.catalogue import SOURCE
from climate_download.cams.state import digest, now


def load(root):
    path = root/'retry_activation.json'
    if not path.is_file():
        return {}
    value = json.loads(path.read_text())
    started = dt.datetime.fromisoformat(value['started_at'])
    if (value['plan_sha256'] != digest(root/'plan.json') or started.tzinfo is None
            or started > dt.datetime.fromisoformat(now()) or not isinstance(value['targets'], dict)):
        raise ValueError('ADS retry activation does not match its plan or clock')
    return {relative: {**target, 'started_at': value['started_at']}
            for relative, target in value['targets'].items()}


def for_state(config, folder, state, original_state_sha256):
    key = folder.relative_to(config.output_dir/SOURCE).as_posix()
    row = config._retry_activations.get(key)
    if row and (row.get('account'), row.get('request_sha256'), row.get('state_sha256')) == (
            state['account'], state['request_sha256'], original_state_sha256):
        return row
    return None


def start(activation, state, original_state_sha256, observed):
    if activation is None:
        return observed
    at = dt.datetime.fromisoformat(activation['started_at'])
    if (at.tzinfo is None or at < observed or at > dt.datetime.fromisoformat(now())
            or (activation['account'], activation['request_sha256'], activation['state_sha256']) != (
                state['account'], state['request_sha256'], original_state_sha256)):
        raise ValueError('ADS retry activation differs from the original failed attempt')
    return at
