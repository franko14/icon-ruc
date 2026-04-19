# ICON-D2-RUC-EPS · Bratislava

Ensemble precipitation and wind-gust forecasts for Bratislava from DWD's ICON-D2-RUC-EPS model. The pipeline downloads GRIB2 files, extracts the single grid cell nearest to Bratislava, computes ensemble percentiles and exceedance probabilities, and writes a JSON file per run. A small Flask API serves the forecasts to a static HTML/uPlot dashboard.

## Install

```bash
python -m venv .venv
.venv/bin/pip install -r requirements.txt
```

### Rust extension (optional, ~11× faster)

The GRIB extraction is also available as a Rust/pyo3 extension using rayon for parallel decoding. Build it once into the venv:

```bash
.venv/bin/pip install maturin
.venv/bin/maturin develop --release --manifest-path extract_rs/Cargo.toml
```

Requires a Rust toolchain (`rustup`) and the `eccodes` C library (macOS: `brew install eccodes`). If the extension isn't built, `pipeline/extract.py` falls back to a pure-Python xarray/cfgrib path automatically.

## Run

```bash
# Process the most recent completed DWD run
.venv/bin/python main.py

# Process N most recent runs
.venv/bin/python main.py --runs 3

# Process one specific run (e.g. one you already have cached in data/raw/)
.venv/bin/python main.py --run-id 2025-10-28T0700

# Fully offline — use only files in data/raw/, never touch DWD
.venv/bin/python main.py --run-id 2025-10-28T0700 --offline

# List cached runs
.venv/bin/python main.py --list-local
```

Downloads are cache-aware: files already in `data/raw/` are never re-fetched. Raw GRIBs are preserved after extraction — use `cleanup.py` to trim them.

## Dashboard

```bash
.venv/bin/python api.py             # serves http://127.0.0.1:5000/
```

Open `http://127.0.0.1:5000/` in a browser. The dashboard reads from `data/forecasts/*.json` via the API.

## Cleanup

```bash
.venv/bin/python cleanup.py --hours 12           # delete GRIBs older than 12h
.venv/bin/python cleanup.py --hours 24 --dry-run # preview only
```

## Tests

```bash
.venv/bin/python -m pytest tests/
```

## Deploy to GitHub Pages (free, 24/7)

The dashboard is fully static once the JSONs exist, so the whole stack is free:
- **GitHub Actions** runs `main.py` every hour at `:45 UTC` (DWD publishes complete RUC-EPS runs ~30–40 min after each init hour)
- **GitHub Pages** serves `dashboard.html` + `data/forecasts/*.json` directly from the repo

**One-time setup**

1. Push this repo to a public GitHub repository (private works too but uses your 2,000 min/month Actions quota).
2. GitHub → Settings → Actions → General → Workflow permissions → **Read and write permissions** (so the scheduled job can commit back).
3. GitHub → Settings → Pages → Source: **Deploy from a branch** · Branch: `main` · Folder: `/ (root)`.
4. Wait a couple of minutes, then your dashboard is live at `https://<username>.github.io/<repo>/`.

The workflow at `.github/workflows/refresh.yml` then:
- Fires hourly at `:45 UTC`
- Installs `libeccodes-dev` + Python deps
- Runs `main.py --runs 1` (downloads ~1,740 GRIBs, extracts the Bratislava cell, writes one JSON)
- Runs `cleanup.py --keep-last 12` to trim old JSONs
- Commits the JSONs back to `main` if anything changed

The dashboard auto-discovers whether it's running against the Flask dev API (local) or static files on Pages, and falls back gracefully between the two. Typical CI run: ~90 seconds.

## Layout

```
pipeline/
  config.py     variable specs, location, URLs, thresholds
  discover.py   remote + local run discovery, URL/filename helpers
  download.py   async aiohttp downloader, cache-aware
  grid.py       ICON grid loader + KDTree, cached on disk
  extract.py    open GRIB → extract single point → close
  stats.py      deaccumulation, percentiles, exceedance probs
  run.py        orchestrator
main.py         CLI
api.py          Flask API + dashboard server
cleanup.py      standalone GRIB cleanup
dashboard.html  uPlot single-page dashboard
tests/          pytest suite
data/
  raw/          GRIB downloads (preserved between runs)
  grid/         ICON grid NetCDF + pickled KDTree
  forecasts/    one JSON file per processed run
```

## Adding a variable

Add one dict entry to `pipeline/config.py`:

```python
VARIABLES = {
    ...,
    "T_2M": {
        "grib_var": "t2m",
        "is_accumulated": False,
        "step_minutes": 60,
        "unit": "K",
        "thresholds": [273.15, 283.15, 293.15],
    },
}
```

The rest of the pipeline works unchanged.

## Forecast JSON shape

```json
{
  "run_id": "2025-10-28T0700",
  "location": {"name": "Bratislava", "lat": 48.1486, "lon": 17.1077},
  "generated_at": "2025-10-28T10:00:00+00:00",
  "grid_distance_km": 1.071,
  "variables": {
    "TOT_PREC": {
      "unit": "mm/h",
      "times": ["2025-10-28T07:00:00Z", ...],
      "ensemble_members": [[...], [...], ...],
      "percentiles": {"p10": [...], "p25": [...], "p50": [...], "p75": [...], "p90": [...]},
      "probability_exceeds": {"0.1": [...], "1.0": [...], "5.0": [...], "10.0": [...]}
    },
    "VMAX_10M": { ... same shape ... }
  }
}
```
