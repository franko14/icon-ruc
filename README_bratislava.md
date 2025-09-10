# Bratislava ICON-D2-RUC-EPS Precipitation Forecasts

Simple, focused workflow for Bratislava precipitation forecasts using ICON-D2-RUC-EPS ensemble data.

## Quick Start

### 1. Run the Pipeline
```bash
python bratislava_pipeline.py
```

This will:
- ✅ Download latest ICON-D2-RUC-EPS data (4 recent runs)
- ✅ Extract Bratislava point (48.1486°N, 17.1077°E) 
- ✅ Properly deaccumulate TOT_PREC → precipitation rates
- ✅ Calculate ensemble statistics (mean, median, percentiles)
- ✅ Save processed data to `data/bratislava/`

### 2. Browse the Results
Open `bratislava_browser.ipynb` in Jupyter to:
- 🎛️ Interactively explore precipitation forecasts
- 📊 View combined and individual run charts
- 📈 Analyze uncertainty with percentile bands
- 🕒 See forecasts on real timestamps

## Features

### Pipeline (`bratislava_pipeline.py`)
- **Smart deaccumulation**: Handles TOT_PREC correctly with automatic time interval detection
- **Full forecast range**: Downloads all available hours (typically 42+ hours)
- **Single point extraction**: Much faster than regridding entire domain
- **Parallel downloads**: Up to 8 concurrent connections
- **Automatic cleanup**: Removes temporary GRIB files

### Browser (`bratislava_browser.ipynb`)
- **Interactive controls**: Select statistical variables and forecast runs
- **Two chart views**: Combined overview + individual runs with uncertainty
- **Timestamp-based x-axis**: Shows actual time (run_time + forecast_hour)
- **Rich statistics**: Mean, median, percentiles (p05-p95), min/max, std dev

## Data Details

### Input: ICON-D2-RUC-EPS
- **Source**: DWD (German Weather Service)
- **Variable**: TOT_PREC (total accumulated precipitation)
- **Resolution**: Native ICON grid (~2.2 km)
- **Ensemble**: 20 members per run
- **Frequency**: Every hour, 5-15 minute time steps

### Output: Precipitation Rates  
- **Location**: Bratislava city center
- **Unit**: mm/h (properly converted from accumulated totals)
- **Statistics**: Full ensemble analysis with percentiles
- **Format**: NetCDF with all metadata

## File Structure
```
data/bratislava/
├── bratislava_precipitation_YYYYMMDD_HHMMSS.nc  # Processed data
└── raw/  # Temporary GRIB files (auto-cleaned)

bratislava_pipeline.py      # Complete processing pipeline
bratislava_browser.ipynb    # Interactive visualization
README_bratislava.md        # This file
```

## Requirements
```bash
pip install xarray netcdf4 pandas numpy plotly ipywidgets tqdm requests cfgrib
```

## Example Output
The pipeline typically processes:
- 🗂️ ~3,000-8,000 GRIB files (depending on forecast length)
- ⏱️ 42+ hours of forecast data
- 📊 20 ensemble members × 4 forecast runs
- 💾 Final dataset: ~1-5 MB (very compact for single point)

Processing time: **5-15 minutes** depending on internet speed.

## Troubleshooting

### Common Issues

**"No forecast runs found"**
- Check internet connection
- DWD servers may be updating (try again in 10-15 minutes)

**"Error extracting point"**
- Install cfgrib: `pip install cfgrib`
- May need eccodes: `conda install -c conda-forge eccodes` 

**"No Bratislava data found"**
- Run `python bratislava_pipeline.py` first
- Check `data/bratislava/` directory exists

### Data Quality
The pipeline automatically handles:
- ✅ Missing time steps
- ✅ Variable time intervals (5min, 15min, etc.)
- ✅ Negative values (set to zero)
- ✅ Network timeouts with retries

## Advanced Usage

### Custom Location
Edit coordinates in `bratislava_pipeline.py`:
```python
BRATISLAVA_COORDS = {'lat': 48.1486, 'lon': 17.1077}  # Change here
```

### More Runs
Increase processing scope:
```python
CONFIG = {
    'num_runs': 6,  # Process 6 runs instead of 4
    # ...
}
```

### Different Percentiles
Customize statistics:
```python
CONFIG = {
    'percentiles': [1, 5, 10, 25, 50, 75, 90, 95, 99],
    # ...
}
```

---

**Created for**: Fast, reliable precipitation forecasts for Bratislava  
**Data source**: DWD ICON-D2-RUC-EPS  
**Processing**: Proper deaccumulation with ensemble statistics  
**Visualization**: Interactive Jupyter browser with timestamps