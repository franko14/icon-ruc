"""Tests for pipeline/extract.py using real local GRIB files (offline)."""
from __future__ import annotations

from pathlib import Path

import pytest

from pipeline import config, discover, extract


def _sample_paths(variable: str, limit: int = 4) -> list[Path]:
    """Pick a handful of real local GRIB files (any run, single ensemble)."""
    local = discover.scan_local_runs()
    for run_id, by_var in local.items():
        files = by_var.get(variable, [])
        # Keep only ensemble "01" to stay fast
        ens01 = [f for f in files if f"_e01_" in f.name]
        if len(ens01) >= limit:
            return ens01[:limit]
    return []


# Use a cell_index known to be inside the ICON-D2 domain (Bratislava area).
# Cell 0 is outside the domain (returns NaN) — that's not what we want to test here.
_VALID_CELL = 100_000


@pytest.mark.skipif(not _sample_paths("TOT_PREC"),
                    reason="no local TOT_PREC GRIB files available")
def test_extract_tot_prec_returns_values_for_valid_cell():
    paths = _sample_paths("TOT_PREC", limit=4)
    result = extract.extract_variable(paths, "TOT_PREC", cell_index=_VALID_CELL)
    assert "01" in result
    assert len(result["01"]) == len(paths)
    for _, v in result["01"]:
        assert v >= 0.0


@pytest.mark.skipif(not _sample_paths("VMAX_10M"),
                    reason="no local VMAX_10M GRIB files available")
def test_extract_vmax_returns_values():
    paths = _sample_paths("VMAX_10M", limit=4)
    result = extract.extract_variable(paths, "VMAX_10M", cell_index=_VALID_CELL)
    assert "01" in result
    assert len(result["01"]) == len(paths)


def test_extract_skips_files_with_nan_at_cell():
    """Cells outside the ICON domain return NaN and should be silently skipped."""
    paths = _sample_paths("TOT_PREC", limit=2)
    if not paths:
        pytest.skip("no local GRIBs")
    result = extract.extract_variable(paths, "TOT_PREC", cell_index=0)
    # Cell 0 is outside the domain -> no values extracted
    assert result == {} or all(len(v) == 0 for v in result.values())
