#!/usr/bin/env python3
"""Backwards-compatible shim around the ``climate-download run`` CLI.

This script used to host the AIFS download CLI directly. The logic now lives
in :mod:`climate_download.cli` and is exposed as the ``climate-download``
console script (see ``[project.scripts]`` in ``pyproject.toml``):

    uv run climate-download run --config config/jobs/aifs_wind_pv.yaml ...

Older invocations through this script still work — we simply prepend the
``run`` subcommand and delegate.
"""

from __future__ import annotations

import sys

from climate_download.cli import main


if __name__ == "__main__":
    sys.exit(main(["run", *sys.argv[1:]]))
