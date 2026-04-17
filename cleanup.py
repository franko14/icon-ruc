#!/usr/bin/env python3
"""Trim old GRIB files from data/raw/ by age."""
from __future__ import annotations

import argparse
import time
from pathlib import Path

from pipeline import config


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--hours", type=float, default=12,
                   help="Delete GRIB files older than N hours (default: 12)")
    p.add_argument("--dry-run", action="store_true",
                   help="List files that would be deleted without removing them")
    args = p.parse_args()

    cutoff = time.time() - args.hours * 3600
    targets = []
    for f in config.RAW_DIR.glob("icon_d2_ruc_eps_*"):
        if f.stat().st_mtime < cutoff:
            targets.append(f)
    targets.sort()

    total_bytes = sum(f.stat().st_size for f in targets)
    label = "would delete" if args.dry_run else "deleting"
    print(f"{label} {len(targets)} files ({total_bytes / 1e9:.2f} GB)")
    if args.dry_run:
        for f in targets[:10]:
            print(f"  {f.name}")
        if len(targets) > 10:
            print(f"  ... and {len(targets) - 10} more")
        return
    for f in targets:
        try:
            f.unlink()
        except OSError as e:
            print(f"  failed to delete {f.name}: {e}")


if __name__ == "__main__":
    main()
