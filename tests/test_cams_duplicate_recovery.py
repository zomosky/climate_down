"""Native coverage, exact duplicate provenance, and legacy failure recovery."""
import json
from pathlib import Path

import pytest

from climate_download.cams import grib, worker
from climate_download.cams.catalogue import VARIABLES
from climate_download.cams.state import digest
from test_cams import header
from test_cams_staged_e2e import system, report


def sample(tmp_path, monkeypatch, *, changed=False, missing=False, foreign=False):
    variable = VARIABLES[0]
    first = (header(variable, 0), b"GRIBfirst7777")
    repeated = (dict(first[0]), b"GRIBother7777" if changed else first[1])
    if foreign:
        repeated[0]["dataDate"] = 20261022
    messages = [first, repeated]
    if not missing:
        messages.append((header(variable, 1), b"GRIBsecond7777"))
    path = tmp_path/"raw.grib"
    path.write_bytes(b"".join(raw for _, raw in messages))
    monkeypatch.setattr(grib, "iter_messages", lambda source: iter(messages))
    return path, variable


def test_exact_duplicates_keep_source_bytes_and_publish_each_identity_once(tmp_path, monkeypatch):
    path, variable = sample(tmp_path, monkeypatch)
    original = path.read_bytes()
    result = grib.validate([path], "20261021", 0, (variable,), native_leads=(0, 1), split_dir=tmp_path)
    assert result["messages"] == 2 and result["source_messages"] == 3
    duplicate, = result["identical_duplicates"]
    assert duplicate["source_message_index"] == 1 and duplicate["original_record_index"] == 0
    assert duplicate["sha256"] == result["records"][0]["sha256"]
    assert (tmp_path/"f000.subset.grib2.part").read_bytes() == b"GRIBfirst7777"
    assert path.read_bytes() == original and result["originals"][0]["sha256"] == digest(path)


@pytest.mark.parametrize("problem", ["changed", "missing", "foreign"])
def test_duplicate_recovery_still_rejects_conflicts_gaps_and_foreign_inits(tmp_path, monkeypatch, problem):
    path, variable = sample(tmp_path, monkeypatch, **{problem: True})
    with pytest.raises(ValueError):
        grib.validate([path], "20261021", 0, (variable,), native_leads=(0, 1))


def test_identical_duplicates_across_independent_inputs_are_rejected(tmp_path, monkeypatch):
    path, variable = sample(tmp_path, monkeypatch)
    other = tmp_path/"other.grib"
    other.write_bytes(path.read_bytes())
    with pytest.raises(ValueError):
        grib.validate([path, other], "20261021", 0, (variable,), native_leads=(0, 1))


@pytest.mark.parametrize("interrupted", [None, "validation", "after_rename"])
def test_saved_duplicate_failure_recovers_original_order_and_archives_failure(system, monkeypatch, interrupted):
    config, clock, client, tmp = system
    config = config.model_copy(update={"hourly_request_mode": "bundle", "max_run_seconds": 0})
    client.failures = 0
    transfer = worker.transfer
    def duplicate_transfer(remote, part, *args, **kwargs):
        receipt, done = transfer(remote, part, *args, **kwargs)
        if len(remote.request["variable"]) == 45:
            original = part.read_bytes()
            part.write_bytes(original + original[:19*19])
            receipt.update(expected_bytes=part.stat().st_size, partial_bytes=part.stat().st_size)
        return receipt, done
    monkeypatch.setattr(worker, "transfer", duplicate_transfer)
    validation = grib.validate
    def old_validation(*args, **kwargs):
        raise ValueError("Duplicate CAMS parameter/native lead (210213, 0)")
    monkeypatch.setattr(worker, "validate", old_validation)
    with pytest.raises(worker.BlockedRequestError):
        worker.run_init(config, "20261027", 0, client=client)
    folder = config.output_dir/"ec_cams/20261027/00z/.cams/hourly"
    failed = (folder/"state.json").read_bytes()
    raw = (folder/"download.grib.part").read_bytes()
    original_id = json.loads(failed)["request_id"]
    get_remote = client.get_remote
    def remaining_order(request_id):
        assert request_id != original_id, 'Complete local raw should recover without another ADS lookup'
        return get_remote(request_id)
    monkeypatch.setattr(client, "get_remote", remaining_order)
    if interrupted == "validation":
        def power_loss(*args, **kwargs):
            raise SystemExit("power loss")
        monkeypatch.setattr(worker, "validate", power_loss)
        with pytest.raises(SystemExit):
            worker.run_init(config, "20261027", 0, client=client)
    elif interrupted == "after_rename":
        save = worker.save
        def power_loss_after_rename(path, value):
            if path == folder/"state.json" and value.get("phase") == "complete":
                raise SystemExit("power loss after raw rename")
            save(path, value)
        monkeypatch.setattr(worker, "validate", validation)
        monkeypatch.setattr(worker, "save", power_loss_after_rename)
        with pytest.raises(SystemExit):
            worker.run_init(config, "20261027", 0, client=client)
        monkeypatch.setattr(worker, "save", save)
    monkeypatch.setattr(worker, "validate", validation)
    result = worker.run_init(config, "20261027", 0, client=client)
    assert result["status"] == "complete" and len(client.posts) == 3
    saved = json.loads((folder/"state.json").read_text())
    archive = folder/saved["duplicate_validation_recovery"]["archive"]
    assert (archive/"state.json").read_bytes() == failed
    assert (folder/"download.grib").read_bytes() == raw
    assert saved["request_id"] == original_id
    receipt = json.loads((Path(result["manifest"]).parent/".cams/raw_validation.json").read_text())
    assert receipt["messages"] == 5732 and receipt["source_messages"] == 5751
    assert len(receipt["identical_duplicates"]) == 19
    report("duplicate-source-recovery-" + str(interrupted), config, client, [result], {
        "messages": 5732, "source_messages": 5751, "exact_duplicates": 19,
        "failed_state_preserved": True, "raw_source_preserved": True,
        "original_hourly_request_id": original_id, "hourly_additional_submissions": 0})
