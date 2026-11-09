"""Independent raw-message acceptance checks the complete duplicate audit."""
import hashlib
import pytest

from test_cams_partition import acceptance_module


def frame(ec, lead, value):
    handle = ec.codes_grib_new_from_samples("regular_ll_sfc_grib2")
    try:
        ec.codes_set(handle, "paramId", 167)
        ec.codes_set(handle, "step", lead)
        ec.codes_set_values(handle, [value]*ec.codes_get_size(handle, "values"))
        return ec.codes_get_message(handle)
    finally:
        ec.codes_release(handle)


@pytest.mark.parametrize("damage", [None, "proof", "values", "unreported", "source_count"])
def test_independent_acceptance_requires_matching_duplicate_bytes_and_receipt(tmp_path, monkeypatch, damage):
    ec = pytest.importorskip("eccodes")
    module = acceptance_module()
    first, second = frame(ec, 0, 280), frame(ec, 1, 281)
    repeated = frame(ec, 0, 285) if damage == "values" else first
    raw = tmp_path/"download.grib"
    raw.write_bytes(first+repeated+second)
    receipt = {"messages": 2, "source_messages": 3, "records": [
        {"param_id": 167, "lead_hours": lead, "bytes": len(payload),
         "sha256": hashlib.sha256(payload).hexdigest()}
        for lead, payload in ((0, first), (1, second))],
        "identical_duplicates": [{"param_id": 167, "lead_hours": 0, "bytes": len(first),
            "sha256": hashlib.sha256(first).hexdigest(), "source_message_index": 1,
            "original_record_index": 0}]}
    if damage == "proof":
        receipt["identical_duplicates"][0]["original_record_index"] = 1
    elif damage == "unreported":
        receipt["identical_duplicates"] = []
    elif damage == "source_count":
        receipt["source_messages"] = 2
    monkeypatch.setattr(module, "request_directory", lambda *args: tmp_path)
    manifest = {"original_requests": [{"group": "hourly"}]}
    if damage:
        with pytest.raises(AssertionError):
            module.original_stream_hashes(tmp_path, manifest, receipt)
    else:
        checksums = module.original_stream_hashes(tmp_path, manifest, receipt)
        assert checksums[0].hexdigest() == hashlib.sha256(first).hexdigest()
        assert checksums[1].hexdigest() == hashlib.sha256(second).hexdigest()
        assert raw.read_bytes() == first+first+second
