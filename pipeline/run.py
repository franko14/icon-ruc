"""Pipeline orchestrator: discover -> download -> extract -> stats -> write JSON."""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

from . import config, discover, download, extract, grid, stats


async def _download_run(variable: str, run_id: str, offline: bool) -> list[Path]:
    """Return the GRIB file paths for a variable+run, fetching missing ones if online."""
    if offline:
        paths = discover.files_for_run(run_id, variable)
        if not paths:
            print(f"  ⚠ {variable} {run_id}: no local files found (offline mode)")
        return paths
    try:
        ensembles = discover.list_remote_ensembles(variable, run_id)
        steps = discover.list_remote_steps(variable, run_id, ensembles[0]) if ensembles else []
    except Exception as e:
        print(f"  ⚠ remote discovery failed for {variable} {run_id}: {e}")
        return discover.files_for_run(run_id, variable)
    if not ensembles or not steps:
        return discover.files_for_run(run_id, variable)
    return await download.fetch_variable(variable, run_id, ensembles, steps)


async def process_run(run_id: str, offline: bool = False) -> Path | None:
    """Process one run: download/load all variables, extract, compute stats, write JSON."""
    print(f"\n── run {run_id} ──")
    tree, lats, _ = grid.load_or_build_index()
    cell_index, distance_km = grid.nearest_index(
        tree, config.LOCATION["lat"], config.LOCATION["lon"]
    )
    print(f"  nearest grid cell: idx={cell_index} ({distance_km:.2f} km from target)")

    output = {
        "run_id": run_id,
        "location": config.LOCATION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "grid_distance_km": round(distance_km, 3),
        "variables": {},
    }
    for var_name in config.VARIABLES:
        paths = await _download_run(var_name, run_id, offline)
        if not paths:
            print(f"  {var_name}: skipped (no files)")
            continue
        print(f"  {var_name}: extracting from {len(paths)} files...")
        series = extract.extract_variable(paths, var_name, cell_index)
        output["variables"][var_name] = stats.build_variable_output(series, var_name)
        print(f"  {var_name}: {len(series)} ensemble members, "
              f"{len(output['variables'][var_name]['times'])} timestamps")

    if not output["variables"]:
        print(f"  ✗ no data for {run_id}")
        return None

    config.ensure_dirs()
    out_path = config.FORECAST_DIR / f"{run_id}.json"
    with open(out_path, "w") as f:
        json.dump(output, f, separators=(",", ":"))
    print(f"  ✓ wrote {out_path}")
    _write_index()
    return out_path


def _write_index() -> None:
    """Emit data/forecasts/index.json — the static catalog the dashboard reads."""
    run_ids = sorted(
        (p.stem for p in config.FORECAST_DIR.glob("*.json") if p.stem != "index"),
        reverse=True,
    )
    idx_path = config.FORECAST_DIR / "index.json"
    with open(idx_path, "w") as f:
        json.dump({"runs": run_ids, "generated_at":
                   datetime.now(timezone.utc).isoformat(timespec="seconds")},
                  f, separators=(",", ":"))


def resolve_runs(run_id: str | None = None, runs: int = 1,
                 offline: bool = False) -> list[str]:
    """Pick the runs to process based on CLI args."""
    if run_id:
        return [run_id]
    if offline:
        return discover.local_run_ids()[:runs]
    try:
        return discover.list_remote_runs(limit=runs)
    except Exception as e:
        print(f"remote discovery failed ({e}); falling back to local runs")
        return discover.local_run_ids()[:runs]


async def process_runs(run_ids: list[str], offline: bool = False) -> list[Path]:
    outputs = []
    for run_id in run_ids:
        out = await process_run(run_id, offline=offline)
        if out:
            outputs.append(out)
    return outputs
