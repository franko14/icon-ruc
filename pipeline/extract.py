"""Single-point extraction from GRIB files."""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import numpy as np
import xarray as xr

from . import config, discover


def _read_point(path: Path, grib_var: str, cell_index: int) -> tuple[np.datetime64, float] | None:
    try:
        ds = xr.open_dataset(path, engine="cfgrib",
                             backend_kwargs={"indexpath": ""})
    except Exception as e:
        print(f"  open failed {path.name}: {e}")
        return None
    try:
        if grib_var not in ds.data_vars:
            grib_var = next(iter(ds.data_vars))
        arr = ds[grib_var].values
        value = float(arr.flat[cell_index])
        if np.isnan(value):
            return None
        time = ds.time.values
        if "step" in ds.coords:
            time = time + ds.step.values
        return time, value
    finally:
        ds.close()


def extract_variable(paths: list[Path], variable: str, cell_index: int
                     ) -> dict[str, list[tuple[np.datetime64, float]]]:
    """Extract point series per ensemble from local GRIB files.

    Returns {ensemble_id: [(time, value), ...]} sorted by time.
    """
    grib_var = config.VARIABLES[variable]["grib_var"]
    by_ens: dict[str, list[tuple[np.datetime64, float]]] = defaultdict(list)

    for path in paths:
        parsed = discover.parse_filename(path)
        if parsed is None or parsed[0] != variable:
            continue
        _, _, ensemble, _ = parsed
        result = _read_point(path, grib_var, cell_index)
        if result is None:
            continue
        by_ens[ensemble].append(result)

    return {ens: sorted(items, key=lambda x: x[0]) for ens, items in by_ens.items()}
