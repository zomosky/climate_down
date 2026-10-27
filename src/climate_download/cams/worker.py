"""Durable, account-locked worker for exactly one full CAMS init at a time."""
from __future__ import annotations

import datetime as dt
import fcntl
import json
from pathlib import Path
import time

from climate_download.cams.catalogue import DATASET, GROUPS, SOURCE, request_for
from climate_download.cams.client import PreflightRejected, create_client, preflight, transfer
from climate_download.cams.config import CamsConfig
from climate_download.cams.grib import publish, validate, verify_published
from climate_download.cams import recovery
from climate_download.cams.state import digest, now, safe_error, save, signature

BLOCKED = {"submitting", "submission_uncertain", "remote_failed", "validation_failed", "preflight_rejected"}
TERMINAL_FAILURES = {"remote_failed", "validation_failed", "preflight_rejected"}


class BlockedRequestError(ValueError):
    """A retained failure needs inspection, while other scheduled inits may run."""


def _submit_once(folder, state, request, client, secret):
    """The intent and retry count are durable before either kind of POST."""
    state.update(phase='submitting', submission_started_at=now())
    state.pop('request_id', None)
    save(folder/'state.json', state)
    try:
        request_id = client.submit(DATASET, request).request_id
        if not request_id:
            raise ValueError('ADS did not return a request ID')
        if any(attempt['request_id'] == request_id for attempt in state.get('previous_attempts', [])):
            raise ValueError('ADS returned a previously failed request ID; inspect the submission')
    except Exception as error:
        state.update(phase='submission_uncertain', error=safe_error(error, secret))
        save(folder/'state.json', state)
        raise BlockedRequestError('ADS submit uncertain; inspect receipt and recover original ID') from None
    state.update(request_id=request_id, phase='submitted', submitted_at=now())
    for field in ('error', 'retry_due_at', 'retry_wait_reason', 'remote_status', 'server_timestamps'):
        state.pop(field, None)
    save(folder/'state.json', state)


def group_tick(config: CamsConfig, folder: Path, date: str, cycle: int, group: str, client, secret: str) -> bool:
    account = config.account_for(cycle).name
    request = request_for(date, cycle, group)
    folder.mkdir(parents=True, exist_ok=True)
    state_path = folder / "state.json"
    state = json.loads(state_path.read_text()) if state_path.exists() else {
        "phase": "prepared", "account": account, "dataset": DATASET, "created_at": now(),
        "request_sha256": signature(request), "group": group, "date": date, "cycle": cycle}
    if state["request_sha256"] != signature(request) or state["account"] != account or state["dataset"] != DATASET:
        raise ValueError("Pinned CAMS request/account differs; reconcile the existing job")
    if state["phase"] in BLOCKED:
        raise BlockedRequestError(f"CAMS {state['phase']} requires inspection; automatic resubmission is forbidden")
    target, part = folder / "download.grib", folder / "download.grib.part"
    if state["phase"] == "complete":
        if not target.is_file() or digest(target) != state["sha256"]:
            raise ValueError("Completed raw CAMS group checksum mismatch")
        return True
    retrying = False
    if state['phase'] == 'retry_wait':
        try:
            decision = recovery.retry_decision(config, folder, state, request, client, secret)
        except recovery.RecoveryBlocked as error:
            state.update(phase='remote_failed', retry_blocked=safe_error(error, secret))
            save(state_path, state)
            raise BlockedRequestError('MARS retry requires inspection; original failure retained') from None
        if decision is None:
            return False
        retrying = decision == 'retry'
    if not state.get("request_id") or retrying:
        # Cost/licence/constraints checks may be retried. Once submitting is
        # persisted, every crash/exception is blocked until original-ID recovery.
        if not retrying:
            state.setdefault('mars_retry_policy', recovery.policy_for(config))
        try:
            state["preflight"] = preflight(client, request, group)
        except Exception as error:
            status = getattr(getattr(error, "response", None), "status_code", None)
            rejected = isinstance(error, PreflightRejected) or status in (400, 401, 403, 404, 422)
            state.update(phase="preflight_rejected" if rejected else "retry_wait" if retrying else "preflight_deferred",
                         last_check=now(), error=safe_error(error, secret))
            save(state_path, state)
            if rejected:
                raise BlockedRequestError("CAMS policy/cost preflight rejected; saved reason requires inspection") from None
            return False
        if retrying:
            state['retry_count'] = state.get('retry_count', 0) + 1
            state.setdefault('previous_attempts', []).append({
                'request_id': state['request_id'], 'phase': 'remote_failed',
                'state_archive': str(folder/state['failed_attempt']['directory']/'state.json'),
                'state_sha256': state['failed_attempt']['state_sha256']})
        else:
            save(folder / "request.json", request)
        _submit_once(folder, state, request, client, secret)
    try:
        remote = client.get_remote(state["request_id"])
        reply = remote.json
    except Exception as error:
        state.update(last_check=now(), error=safe_error(error, secret))
        inaccessible = getattr(getattr(error, 'response', None), 'status_code', None) in (400, 401, 403, 404, 422)
        if inaccessible:
            state['phase'] = 'remote_failed'
        save(state_path, state)
        if inaccessible:
            raise BlockedRequestError('Pinned ADS job is inaccessible; inspect the original ID/account before recovery') from None
        return False
    status = reply["status"]
    state.update(last_check=now(), remote_status=status,
                 server_timestamps={k: reply.get(k) for k in ("created", "started", "finished", "updated")})
    if status in ("failed", "rejected", "dismissed", "deleted"):
        failure = RuntimeError('ADS returned a terminal failure without extraction diagnostics')
        try:
            remote.get_results()
        except Exception as error:
            failure = error
        if recovery.record_failure(config, folder, state, request, reply, failure, secret):
            return False
        raise BlockedRequestError("ADS job failed; saved sanitized reason, no resubmission")
    if status != "successful":
        state.update(phase="waiting_for_remote")
        save(state_path, state)
        return False
    state.setdefault("ready_observed_at", now())
    state.setdefault("first_download_started_at", now())
    state.update(phase="downloading")
    save(state_path, state)
    try:
        receipt, done = transfer(remote, part, config, expected_bytes=state.get("expected_bytes"))
    except Exception as error:
        state.update(last_transfer_check=now(), error=safe_error(error, secret))
        save(state_path, state)
        raise ValueError("CAMS transfer failed; same request and partial retained") from None
    state.setdefault("transfer_attempts", []).append(receipt)
    state.update(last_transfer_check=now(), partial_bytes=receipt["partial_bytes"],
                 expected_bytes=receipt["expected_bytes"])
    if not done:
        save(state_path, state)
        return False
    state.update(phase="validating", download_finished_at=now())
    save(state_path, state)
    try:
        validation = validate([part], date, cycle, GROUPS[group])
        save(folder / "validation.json", validation)
    except Exception as error:
        state.update(phase="validation_failed", error=safe_error(error, secret))
        save(state_path, state)
        raise BlockedRequestError("CAMS GRIB validation failed; raw bytes retained") from None
    part.replace(target)
    state.update(phase="complete", completed_at=now(), sha256=digest(target),
                 bytes=target.stat().st_size, messages=validation["messages"])
    state.pop("error", None)
    save(state_path, state)
    return True


def run_init(config: CamsConfig, date: str, cycle: int, *, client=None, secret: str = "") -> dict:
    key = f"{date}{cycle:02d}"
    if key < config.first_init:
        raise ValueError("Init predates the configured production floor; no historical backfill")
    # request_for also validates the date before any filesystem/network work.
    request_for(date, cycle, "hourly")
    folder = config.output_dir / SOURCE / date / f"{cycle:02d}z"
    lock_dir = config.output_dir / SOURCE / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    with (lock_dir / f"account-{config.account_for(cycle).name}.lock").open("a") as account_lock, \
            (lock_dir / f"init-{key}.lock").open("a") as init_lock:
        try:
            fcntl.flock(account_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            fcntl.flock(init_lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            return {"status": "locked", "date": date, "cycle": cycle}
        if verify_published(folder, date, cycle):
            return {"status": "complete", "date": date, "cycle": cycle, "resumed": True}
        if client is None:
            try:
                client, secret = create_client(config.account_for(cycle))
            except Exception:
                raise ValueError("Unable to initialize CAMS client; check credential file and installed cams extra") from None
        began = time.monotonic()
        verified_groups = set()
        while True:
            pending = None
            for group in GROUPS:
                if group in verified_groups:
                    continue
                if not group_tick(config, folder / ".cams" / group, date, cycle, group, client, secret):
                    pending = group
                    state = json.loads((folder / ".cams" / group / "state.json").read_text())
                    # Pulse the generic disk monitor only after a real poll
                    # or transfer. A blocked failure must not look alive just
                    # because cron repeatedly encounters it.
                    save(folder / "cams.status.json", {k: state.get(k) for k in (
                        "date", "cycle", "group", "account", "phase", "last_check", "remote_status", "partial_bytes")})
                    break
                verified_groups.add(group)
            if pending is None:
                raw_paths, requests = [], []
                for group in GROUPS:
                    group_dir = folder / ".cams" / group
                    state = json.loads((group_dir / "state.json").read_text())
                    raw_paths.append(group_dir / "download.grib")
                    requests.append({"group": group, "request_id": state["request_id"],
                                     "account": state["account"], "request_sha256": state["request_sha256"]})
                manifest = publish(raw_paths, folder, date, cycle, requests)
                return {"status": "complete", "date": date, "cycle": cycle, "manifest": str(manifest)}
            if time.monotonic() - began >= config.max_run_seconds:
                return {"status": "pending", "date": date, "cycle": cycle, "group": pending}
            time.sleep(config.poll_seconds)


def scheduled_inits(config: CamsConfig, cycle: int, at) -> list[str]:
    """Resume saved jobs even after the recent *new submission* window moves on."""
    from climate_download.cams.config import candidate_inits
    keys = set(candidate_inits(config, cycle, at))
    source_dir = config.output_dir / SOURCE
    # Explicit recovery receipts keep finite gaps eligible after the recent
    # submission window moves on, without widening production into history.
    for path in source_dir.glob(f"????????/{cycle:02d}z/cams.catchup.json"):
        date = path.parents[1].name
        key = f"{date}{cycle:02d}"
        if key < config.first_init:
            continue
        receipt = json.loads(path.read_text())
        if (receipt.get("date") != date or receipt.get("cycle") != cycle
                or not receipt.get("authorized_at")):
            raise ValueError("CAMS catchup receipt identity differs from its directory")
        request_for(date, cycle, "hourly")
        due = at.astimezone(dt.timezone.utc) - dt.timedelta(hours=config.publish_lag_hours)
        if key <= due.strftime("%Y%m%d%H") and not (path.parent / f"{date}_{cycle:02d}z_{SOURCE}.manifest.json").is_file():
            keys.add(key)
    for path in source_dir.glob(f"????????/{cycle:02d}z/.cams/*/state.json"):
        state = json.loads(path.read_text())
        product_folder = path.parents[2]
        has_intent = state.get('request_id') or state.get('phase') in ('submitting', 'submission_uncertain')
        manifest = product_folder / f"{state['date']}_{cycle:02d}z_{SOURCE}.manifest.json"
        if has_intent and (state.get('phase') != 'complete' or not manifest.is_file()):
            key = f"{state['date']}{cycle:02d}"
            if state.get("cycle") != cycle:
                raise ValueError("Saved CAMS init is outside configured cycle ownership")
            # Historical requests belong to their explicit bounded runner.
            # A normal cron scan must neither adopt them nor block live inits.
            if key < config.first_init:
                continue
            keys.add(key)
    return sorted(keys)


def pending_inits(config: CamsConfig, cycle: int, at) -> list[str]:
    """Live priority excludes terminal failures but retains uncertain submissions."""
    pending = []
    for key in scheduled_inits(config, cycle, at):
        folder = config.output_dir / SOURCE / key[:8] / f"{cycle:02d}z"
        if (folder / f"{key[:8]}_{cycle:02d}z_{SOURCE}.manifest.json").is_file():
            continue
        states = [json.loads(p.read_text()) for p in (folder / ".cams").glob("*/state.json")]
        if any(s.get("phase") in TERMINAL_FAILURES for s in states):
            continue
        pending.append(key)
    return pending
