"""Shared pytest fixtures for grib index/partial tests."""

from __future__ import annotations

from pathlib import Path

import pytest

# Path to the AIFS sample shipped with the wider repository. Tests that depend
# on it are skipped when running from a checkout where the file is absent.
_SAMPLE_DIR = (
    Path(__file__).resolve().parents[2]
    / "cliamte_data"
    / "aifs"
)
_SAMPLE_BASENAME = (
    "20260501_12z_aifs-single_0p25_oper_20260501120000-0h-oper-fc"
)


@pytest.fixture(scope="session")
def aifs_index_path() -> Path:
    path = _SAMPLE_DIR / f"{_SAMPLE_BASENAME}.index"
    if not path.is_file():
        pytest.skip(f"AIFS .index sample not present at {path}")
    return path


@pytest.fixture(scope="session")
def aifs_grib_path() -> Path:
    path = _SAMPLE_DIR / f"{_SAMPLE_BASENAME}.grib2"
    if not path.is_file():
        pytest.skip(f"AIFS GRIB sample not present at {path}")
    return path


@pytest.fixture
def synthetic_grib_bytes() -> bytes:
    """A 1 KiB deterministic byte blob standing in for a remote GRIB file."""
    return bytes(i % 251 for i in range(1024))
