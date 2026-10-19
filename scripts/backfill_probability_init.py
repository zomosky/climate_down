#!/usr/bin/env python3
"""Acquire one historical init; stdout ends with a machine-readable receipt."""
import argparse
import json
from pathlib import Path

from climate_download.probability_history import download_init

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", choices=("ifs", "aifs-ens"), required=True)
    parser.add_argument("--date", required=True)
    parser.add_argument("--cycle", type=int, required=True)
    parser.add_argument("--raw-root", type=Path, default=Path("/climate_data"))
    parser.add_argument("--workers", type=int, choices=range(1, 9), default=4)
    parser.add_argument("--layout", help="Exact historical model/grid path from the inventory")
    print(json.dumps(download_init(**vars(parser.parse_args())), ensure_ascii=False))
