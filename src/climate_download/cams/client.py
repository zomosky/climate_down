"""One-attempt ADS submission, read-only preflight and secret-free transfer."""
from __future__ import annotations

import json
from pathlib import Path
import subprocess
import time
from urllib.parse import urlsplit

from climate_download.cams.catalogue import DATASET, GROUPS
from climate_download.cams.config import Account, CamsConfig
from climate_download.cams.state import now


class PreflightRejected(ValueError):
    """Terms/cost contract needs intervention, rather than endless queue polling."""


class NotAvailableYet(ValueError):
    """An operational init has not exposed the complete native horizon yet."""


def create_client(account: Account):
    try:
        return _create_client(account)
    except Exception:
        # A YAML parser can include the offending credential line in its
        # exception. Never render constructor/parser errors, even before the
        # token is known well enough for value-based redaction.
        raise ValueError("Unable to initialize ADS client; verify credential file and cams extra") from None


def _create_client(account: Account):
    # No environment/system proxy changes. Metadata and raw transfer are direct,
    # as on the previously accepted 241 CAMS experiment.
    import ecmwf.datastores
    import requests
    import yaml

    credential = yaml.safe_load(account.credential_file.expanduser().read_text())
    secret = str(credential["key"])
    session = requests.Session()
    session.trust_env = False
    client = ecmwf.datastores.Client(
        url="https://ads.atmosphere.copernicus.eu/api", key=secret, session=session,
        timeout=(10, 35), maximum_tries=1, retry_after=1, progress=False,
        cleanup=False, log_callback=lambda *args, **kwargs: None,
    )
    return client, secret


def preflight(client, request: dict, group: str) -> dict:
    """Never accepts terms or creates a retrieval; unavailable fields block POST."""
    client.get_jobs(limit=1)
    licences = client._profile_api.accepted_licences()
    if not any(x.get("id") == "cc-by" and x.get("revision") == 1
               for x in licences.get("licences", [])):
        raise PreflightRejected("ADS CC-BY licence has not been accepted")
    constraints = client.get_collection(DATASET).constraints
    for variable in GROUPS[group]:
        rows = [r for r in constraints if variable.api_name in r.get("variable", [])
                and "forecast" in r.get("type", [])]
        if variable.pressure_level:
            rows = [r for r in rows if str(variable.pressure_level) in r.get("pressure_level", [])]
        else:
            rows = [r for r in rows if not r.get("pressure_level") and not r.get("model_level")]
        leads = {int(h) for r in rows if request["time"][0] in r.get("time", [])
                 and any(span.split("/")[0] <= request["date"] <= span.split("/")[-1]
                         for span in r.get("date", []))
                 for h in r.get("leadtime_hour", [])}
        if not set(variable.leads) <= leads:
            raise NotAvailableYet(f"Full native leads not yet available: {variable.api_name}")
    cost = client.estimate_costs(DATASET, request)
    expected = sum(len(v.leads) for v in GROUPS[group])
    if cost.get("cost") != expected or expected > cost.get("limit", 0):
        raise PreflightRejected("ADS cost/field limit differs from the frozen CAMS request")
    return {"checked_at": now(), "policy_gate": "passed", "cc_by_revision": 1,
            "native_fields_available": True, "cost": cost["cost"], "limit": cost["limit"]}


def transfer(remote, part: Path, config: CamsConfig, *, expected_bytes: int | None = None) -> tuple[dict, bool]:
    """One resumable bounded attempt; signed URL exists only in curl stdin."""
    results = remote.get_results()
    asset = results.asset
    url = asset.get("href") or getattr(results, "location", None)
    expected = int(asset.get("file:size") or getattr(results, "content_length", 0))
    if not url or urlsplit(url).scheme != "https" or not 0 < expected <= config.max_asset_bytes:
        raise ValueError("Missing HTTPS asset metadata or asset exceeds configured bound")
    if expected_bytes is not None and expected != expected_bytes:
        raise ValueError("The pinned ADS asset length changed; do not append a different object")
    before = part.stat().st_size if part.exists() else 0
    if before > expected:
        raise ValueError("Partial exceeds the same request's expected byte length")
    if before == expected:
        return {"expected_bytes": expected, "partial_bytes": before, "already_transferred": True}, True
    # Never place URLs/tokens in process argv, a saved receipt, or a log.
    curl_config = 'url = "' + url.replace("\\", "\\\\").replace('"', '\\"') + '"\n'
    started = time.monotonic()
    result = subprocess.run(
        ["curl", "-q", "--silent", "--show-error", "--location", "--fail",
         "--continue-at", "-", "--connect-timeout", "20", "--max-time", str(config.transfer_seconds),
         "--noproxy", "*", "--output", str(part), "--config", "-", "--write-out", "%{json}"],
        input=curl_config, text=True, capture_output=True, timeout=config.transfer_seconds + 15,
    )
    after = part.stat().st_size if part.exists() else 0
    if after < before or after > expected:
        raise ValueError("Unsafe partial file size change")
    try:
        details = json.loads(result.stdout)
    except ValueError:
        details = {}
    receipt = {"at": now(), "expected_bytes": expected, "partial_bytes": after,
               "bytes_before": before, "elapsed_seconds": round(time.monotonic() - started, 3),
               "returncode": result.returncode,
               "http": {k: details.get(k) for k in ("http_code", "size_download", "speed_download",
                                                      "time_total", "time_starttransfer")}}
    return receipt, after == expected
