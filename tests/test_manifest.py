from climate_download.manifest import _product_signature


def test_product_signature_changes_when_manifest_file_size_is_corrected():
    original = {
        "files": [
            {
                "step_hours": 0,
                "path": "/climate_data/dwd-icon/20260913/12z/f000.subset.grib2",
                "size_bytes": 50,
                "sha256": "same-content",
            }
        ],
        "failures": [],
    }
    corrected = {
        "files": [{**original["files"][0], "size_bytes": 52}],
        "failures": [],
    }

    assert _product_signature(original) != _product_signature(corrected)
