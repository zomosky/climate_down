"""Real production read-only acceptance. Run with restore Python for `raw`,
and with watcher Python for `http`. All receipts/exports stay in validation.
Never submits requests, changes source data, registers profiles or relaxes checks.
"""
from __future__ import annotations

import argparse
from collections import defaultdict
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import time
from urllib.request import Request, urlopen

import numpy as np
import xarray as xr

BASE = Path(os.environ.get("CAMS_ACCEPT_ROOT", "/climate_data/_validation/cams-weekly-20261027/acceptance"))
RAW = Path("/climate_data/ec_cams")
ZARR = Path("/climate_data/climate_data_storage/zarr/ec_cams")
ENERGY = {169, 228129, 228021, 228022, 47, 176, 175, 177, 179}
PRECIP = {228, 143}
SPARSE = {228028, 129, 130, 131, 132, 133, 157}
SITES = [{"id": "beijing", "lat": 39.9, "lon": 116.4},
         {"id": "shanxi", "lat": 37.87, "lon": 112.55},
         {"id": "qinghai", "lat": 36.62, "lon": 101.77}]


def save(path, obj):
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False) + "\n")
    tmp.replace(path)


def digest(path):
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(8 << 20), b""):
            h.update(block)
    return h.hexdigest()


def combine(parts, expected):
    """CAMS gust has scalar h0 instant + 3..120h max: join disjoint cubes."""
    refs, seen, partitions = [], set(), []
    param, units = parts[0].attrs["GRIB_paramId"], parts[0].attrs["units"]
    for part in parts:
        assert part.attrs["GRIB_paramId"] == param and part.attrs["units"] == units
        ref = part if "step" in part.dims else part.expand_dims("step")
        hours = ref.step.values / np.timedelta64(1, "h")
        np.testing.assert_array_equal(hours, hours.astype(int))
        leads = hours.astype(int).tolist()
        assert len(leads) == len(set(leads)) and set(leads) <= expected and not seen.intersection(leads)
        if refs:
            assert ref.dims == refs[0].dims
        seen.update(leads)
        refs.append(ref)
        partitions.append({"step_type": part.attrs["GRIB_stepType"], "native_hours": leads})
    assert seen == expected
    result = (xr.concat(refs, dim="step", coords="all", join="exact", compat="equals",
                        combine_attrs="drop_conflicts") if len(refs) > 1 else refs[0]).sortby("step")
    return result, partitions


def request_directory(folder, request):
    """Resolve literal order bytes; a partition is never a second group bundle."""
    assert request['group'] in ('hourly', 'gust', 'pressure850')
    group = folder/'.cams'/request['group']
    if 'part' not in request:
        return group
    part = request['part']
    assert request['group'] == 'hourly' and isinstance(part, str) and len(part) == 16
    assert all(c in '0123456789abcdef' for c in part)
    plan_path = group/'partition.json'
    parent = json.loads((group/'state.json').read_text())
    assert parent['partitioned_request'] and digest(plan_path) == parent['partition_sha256']
    plan = json.loads(plan_path.read_text())
    item = next(p for p in plan['parts'] if p['id'] == part)
    payload = json.loads((group/'parts'/part/'request.json').read_text())
    assert payload == item['request']
    actual = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
    assert actual == request['request_sha256'] and part == actual[:16]
    return group/'parts'/part


def check_raw(date, cycle):
    import cfgrib

    started = time.monotonic()
    folder = RAW / date / f"{cycle:02d}z"
    manifest = json.loads((folder / f"{date}_{cycle:02d}z_ec_cams.manifest.json").read_text())
    assert manifest["source"]["name"] == "ec_cams" and manifest["completed_at"] and not manifest["failures"]
    assert len(manifest["variables"]) == 52 and len(manifest["files"]) == 121
    assert [f["step_hours"] for f in manifest["files"]] == list(range(121))
    assert sum(f["records_selected"] for f in manifest["files"]) == 5732
    for item in manifest["files"]:
        path = Path(item["path"])
        assert path.stat().st_size == item["size_bytes"] and digest(path) == item["sha256"]
    receipt = json.loads((folder / ".cams/raw_validation.json").read_text())
    assert receipt["passed"] and receipt["messages"] == 5732 and receipt["all_original_bytes_accounted_for"]
    for original in receipt['originals']:
        path = Path(original['path'])
        assert path.stat().st_size == original['size_bytes'] and digest(path) == original['sha256']
    seen = {(r["param_id"], r["lead_hours"]) for r in receipt["records"]}
    expected = {(v["params"][0], h) for v in manifest["variables"] for h in v["native_step_hours"]}
    assert seen == expected and len(receipt["records"]) == len(seen) == 5732
    # Independently hash the literal original-message streams into each step;
    # metadata/value equality alone does not establish byte-preserving splitting.
    import eccodes as ec
    split_hashes = {h: hashlib.sha256() for h in range(121)}
    position = 0
    for request in manifest["original_requests"]:
        raw = request_directory(folder, request) / 'download.grib'
        original_hash = hashlib.sha256()
        with raw.open("rb") as stream:
            while (handle := ec.codes_grib_new_from_file(stream)) is not None:
                try:
                    p, h = int(ec.codes_get(handle, "paramId")), int(ec.codes_get(handle, "endStep"))
                    payload = ec.codes_get_message(handle)
                    record = receipt["records"][position]
                    position += 1
                    assert (record["param_id"], record["lead_hours"]) == (p, h)
                    assert record["sha256"] == hashlib.sha256(payload).hexdigest()
                    original_hash.update(payload)
                    split_hashes[h].update(payload)
                finally:
                    ec.codes_release(handle)
        assert original_hash.hexdigest() == digest(raw)
    assert position == 5732
    for item in manifest["files"]:
        assert split_hashes[item["step_hours"]].hexdigest() == item["sha256"]
    init = np.datetime64(dt.datetime.strptime(date, "%Y%m%d").replace(hour=cycle), "ns")
    product = ZARR / f"{date}_{cycle:02d}z_ec_cams.zarr"
    proof, negatives = [], {}
    with xr.open_zarr(product, consolidated=True) as stored:
        np.testing.assert_array_equal(stored.step.values, np.arange(121).astype("timedelta64[h]"))
        assert stored.time.values == init
        np.testing.assert_array_equal(stored.valid_time.values, init + stored.step.values)
        names = {int(a.attrs["GRIB_paramId"]): name for name, a in stored.data_vars.items()}
        assert len(names) == len(stored.data_vars) == 52 and set(names) == {p for p, _ in expected}
        for group_name in dict.fromkeys(request['group'] for request in manifest['original_requests']):
            cubes = []
            try:
                grouped = defaultdict(list)
                for request in manifest['original_requests']:
                    if request['group'] != group_name:
                        continue
                    group = request_directory(folder, request)
                    state = json.loads((group/'state.json').read_text())
                    assert state['phase'] == 'complete' and state['request_id'] == request['request_id']
                    assert state['account'] == request['account'] and state['request_sha256'] == request['request_sha256']
                    raw = group/'download.grib'
                    assert digest(raw) == state['sha256']
                    opened = cfgrib.open_datasets(str(raw), backend_kwargs={'indexpath': ''})
                    cubes.extend(opened)
                    for ds in opened:
                        for v in ds.data_vars:
                            a = ds[v].sortby('latitude')
                            if 'isobaricInhPa' in a.coords:
                                if 'isobaricInhPa' not in a.dims:
                                    a = a.expand_dims('isobaricInhPa')
                                a = a.rename({'isobaricInhPa': 'pressure_level'})
                            grouped[int(a.attrs['GRIB_paramId'])].append(a)
                for p, parts in grouped.items():
                    leads = set(range(0, 121, 3 if p in SPARSE else 1))
                    ref, partitions = combine(parts, leads)
                    actual = stored[names[p]].sel(step=ref.step).transpose(*ref.dims).load()
                    np.testing.assert_array_equal(actual.values, ref.values)
                    assert actual.attrs["units"] == ref.attrs["units"]
                    for coord in ("step", "latitude", "longitude"):
                        np.testing.assert_array_equal(actual[coord].values, ref[coord].values)
                    other = sorted(set(range(121)) - leads)
                    if other:
                        assert stored[names[p]].sel(step=np.array(other, dtype="timedelta64[h]")).isnull().all()
                    kind = "accum" if p in ENERGY | PRECIP else "max" if p == 228028 else "instant"
                    assert actual.attrs["GRIB_stepType"] == kind
                    proof.append({"param_id": p, "name": names[p], "native_samples": len(leads),
                                  "units": actual.attrs["units"], "partitions": partitions,
                                  "raw_to_zarr_max_abs_difference": 0.0})
                    if p in ENERGY | PRECIP:
                        a = ref.transpose("step", *[d for d in ref.dims if d != "step"]).values.astype("float64")
                        assert np.all(a[0] == 0)
                        delta = np.diff(a, axis=0)
                        negatives[str(p)] = {"minimum_native_increment": float(np.nanmin(delta)),
                                             "negative_increments": int((delta < 0).sum()), "source_values_changed": False}
            finally:
                for ds in cubes:
                    ds.close()
    assert len(proof) == 52
    result = {"passed": True, "date": date, "cycle": cycle, "messages": 5732, "variables": proof,
              "source_negative_increments": negatives, "raw_sha256_and_split_bytes_verified": True,
              "native_times_zero_shift": True, "zarr_exact": True,
              "seconds": round(time.monotonic() - started, 3)}
    save(BASE / f"{date}-{cycle:02d}" / "raw_acceptance.json", result)
    return result


def check_http(date, cycle):
    started = time.monotonic()
    # The production service's data mount is read-only. Keep temporary exports
    # in /tmp and copy only the accepted small artifacts back to the host.
    out = Path(os.environ.get("CAMS_EXPORT_ROOT", "/tmp/cams-weekly-20261027")) / f"{date}-{cycle:02d}"
    out.mkdir(parents=True, exist_ok=True)
    base = "http://127.0.0.1:8090/api/v1"
    def get(path):
        with urlopen(base + path, timeout=180) as response:
            assert response.status == 200
            return json.load(response)
    health = get("/health")
    assert health["status"] == "ok"
    catalog = get("/catalog/variables?source=ec_cams")
    save(out / "catalog.json", catalog)
    with xr.open_zarr(ZARR / f"{date}_{cycle:02d}z_ec_cams.zarr", consolidated=True) as stored:
        exposed = {int(a.attrs["GRIB_paramId"]): name + ("_850" if "pressure_level" in a.dims else "")
                   for name, a in stored.data_vars.items()}
        native_names = {int(a.attrs["GRIB_paramId"]): name for name, a in stored.data_vars.items()}
        variables = list(exposed.values())
        assert catalog["source"] == "ec_cams"
        assert set(variables) <= {v["name"] for v in catalog["variables"]}
        init = dt.datetime.strptime(date, "%Y%m%d").replace(hour=cycle).isoformat()
        time_spec = {"type": "operation", "start": init + "Z", "tz": "UTC", "horizon_hours": 120 + 1 / 3600,
                     "init_date": init[:10], "init_cycle": cycle}
        exports = []
        def query(label, *, source="ec_cams", selected=None, space=None, post=None, sources=None, time_override=None):
            body = {"time": time_override or time_spec, "space": space or {"kind": "sites", "sites": SITES},
                    "postprocess": post or {}}
            if sources is not None:
                body["sources"] = sources
            else:
                body.update(source=source, variables=selected or variables)
            save(out / f"{label}.request.json", body)
            request = Request(base + "/query?format=netcdf", data=json.dumps(body).encode(),
                              headers={"Content-Type": "application/json"}, method="POST")
            with urlopen(request, timeout=180) as response:
                assert response.status == 200
                data = response.read()
                warning_count = response.headers.get("X-Watcher-Warning-Count")
            path = out / f"{label}.nc"
            path.write_bytes(data)
            with xr.open_dataset(path) as opened:
                ds = opened.load()
            exports.append({"file": path.name, "sha256": digest(path), "bytes": len(data),
                            "warning_count": warning_count, "netcdf_readback": True})
            return ds
        region_bbox = [115.2, 116.0, 39.2, 40.0]
        region = query("raw_region", space={"kind": "bbox", "bbox": region_bbox}, post={"deaccumulate": False})
        for p, name in exposed.items():
            ref = stored[native_names[p]].sel(longitude=slice(region_bbox[0], region_bbox[1]),
                                             latitude=slice(region_bbox[2], region_bbox[3]))
            if "pressure_level" in ref.dims:
                ref = ref.sel(pressure_level=850, drop=True)
            ref = ref.swap_dims({"step": "valid_time"})
            np.testing.assert_array_equal(region[name].transpose(*ref.dims).values, ref.values)
            np.testing.assert_array_equal(region.valid_time.values, ref.valid_time.values)
            assert region[name].attrs["units"] == ref.attrs["units"]
        raw = query("raw_sites", post={"deaccumulate": False})
        flux = query("native_flux")
        times = np.datetime64(init) + np.arange(121) * np.timedelta64(1, "h")
        np.testing.assert_array_equal(raw.valid_time, times)
        np.testing.assert_array_equal(flux.valid_time, times)
        for p, name in exposed.items():
            if p not in ENERGY | PRECIP:
                np.testing.assert_array_equal(raw[name].values, flux[name].values)
                continue
            energy = stored[native_names[p]].transpose("step", "latitude", "longitude").load()
            values = energy.values
            expected = np.full(values.shape, np.nan, dtype="float64")
            expected[1:] = np.diff(values.astype("float64"), axis=0) / 3600
            ref = energy.copy(data=expected.astype(values.dtype)).interp(
                latitude=xr.DataArray([s["lat"] for s in SITES], dims="site"),
                longitude=xr.DataArray([s["lon"] for s in SITES], dims="site")).transpose("step", "site").values
            if p in PRECIP and flux[name].attrs["units"] in ("mm/h", "mm h-1", "mm h**-1"):
                ref *= 3600 * (1000 if raw[name].attrs["units"] == "m" else 1)
            np.testing.assert_allclose(flux[name].transpose("valid_time", "site"), ref, rtol=3e-6, atol=2e-5, equal_nan=True)
            assert flux[name].isel(valid_time=0).isnull().all()
        # UTC dpn math: the following UTC day selects this init with
        # n=1 for cycle 00 and n=0 for cycle 12, for every backfill date.
        next_day = (dt.datetime.strptime(date, "%Y%m%d") + dt.timedelta(days=1)).date().isoformat()
        train = query("train_day", time_override={"type": "train", "start": next_day, "end": next_day,
                                                  "cycle": cycle, "n": 1 if cycle == 0 else 0, "tz": "UTC"})
        expected_train = flux.sel(valid_time=slice(next_day + "T00:00:00", next_day + "T23:00:00"))
        np.testing.assert_array_equal(train.valid_time, np.datetime64(next_day) + np.arange(24) * np.timedelta64(1, "h"))
        for name in variables:
            np.testing.assert_array_equal(train[name], expected_train[name])
        np.testing.assert_array_equal(train.init_time, expected_train.init_time)
        fine = query("sites_15min", post={"resample": {"freq": "15min"}})
        fine_times = np.datetime64(init) + np.arange(481) * np.timedelta64(15, "m")
        np.testing.assert_array_equal(fine.valid_time, fine_times)
        assert fine[exposed[169]].isel(valid_time=0).isnull().all()
        assert np.isfinite(fine[exposed[169]].isel(valid_time=slice(1, None))).all()
        np.testing.assert_allclose(fine[exposed[210207]], raw[exposed[210207]].interp(valid_time=fine.valid_time),
                                   rtol=2e-6, atol=1e-7, equal_nan=True)
        for p, name in exposed.items():
            native_hours = np.arange(0, 121, 3 if p in SPARSE else 1)
            if np.isfinite(raw[name].isel(valid_time=native_hours)).all():
                keep = (np.arange(481) > 0) if name in ("ssrd_accum", "ssrdc_accum") else \
                       ((np.arange(481) >= 2) & (np.arange(481) <= 478)) if p in ENERGY | PRECIP else np.ones(481, bool)
                assert np.isfinite(fine[name].isel(valid_time=np.flatnonzero(keep))).all(), name
        ifs_product = ZARR.parent/'ifs-hres'/f'{date}_{cycle:02d}z_ifs-hres.zarr/.zmetadata'
        multi_source_checked = ifs_product.is_file()
        if multi_source_checked:
            selections = {"ec_cams": [exposed[169], exposed[210207], "t2m", "u10", "v10"],
                          "ifs-hres": ["ssrd_accum", "t2m", "u10", "v10"]}
            multi = query("multisource_15min", sources=selections, post={"resample": {"freq": "15min"}})
            for source, names in selections.items():
                single = fine if source == "ec_cams" else query("ifs_15min", source=source, selected=names,
                                                                     post={"resample": {"freq": "15min"}})
                for name in names:
                    np.testing.assert_array_equal(multi[f"{source}:{name}"], single[name])
                np.testing.assert_array_equal(multi[f"{source}:init_time"], single.init_time)
                assert np.all(single.init_time.values == np.datetime64(init))
        result = {"passed": True, "date": date, "cycle": cycle, "http_service": base,
                  "variable_count": len(variables), "region_raw_exact": True, "site_count": len(SITES),
                  "native_samples": 121, "resampled_samples": 481, "zero_shift": True,
                  "energy_independent_flux_passed": True, "h0_missing_preserved": True,
                  "train_and_operation_same_init_values_equal": True,
                  "aod_not_differenced": True, "sparse_native_missingness_preserved": True,
                  "no_unexpected_interior_missingness": True, "multi_source_values_and_provenance_equal": True if multi_source_checked else None,
                  "multi_source_checked": multi_source_checked,
                  "multi_source_unavailable_reason": None if multi_source_checked else "Matching IFS init is absent; CAMS acceptance is independent",
                  "solar_corrections": {name: a.attrs["zrread_solar_corrections"] for name, a in fine.data_vars.items()
                                         if a.attrs.get("zrread_solar_corrections")},
                  "exports": exports, "seconds": round(time.monotonic() - started, 3)}
    save(out / "http_acceptance.json", result)
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("stage", choices=("raw", "http"))
    parser.add_argument("--date", required=True)
    parser.add_argument("--cycle", required=True, type=int, choices=(0, 12))
    args = parser.parse_args()
    result = check_raw(args.date, args.cycle) if args.stage == "raw" else check_http(args.date, args.cycle)
    print(json.dumps({k: v for k, v in result.items() if k not in ("variables", "exports", "solar_corrections")},
                     ensure_ascii=False, allow_nan=False))
