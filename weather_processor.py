#!/usr/bin/env python3
"""
Simplified Weather Data Processor
=================================

Single-function processor that downloads and processes both precipitation and wind data
from DWD ICON-D2-RUC-EPS forecasts for a specific location.

Outputs directly to JSON format ready for web visualization.
"""

import os
import json
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import xarray as xr
from datetime import datetime, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import logging
from typing import Dict, List, Optional, Tuple
import tempfile
import shutil
import argparse
from functools import lru_cache
import time

def parse_timestamp_flexible(time_str):
    """Parse timestamps handling both old (2025-08-31T06:05:00) and new formats"""
    if 'T_' in time_str:
        # Convert 2025-08-31T_0605 to 2025-08-31T06:05:00
        parts = time_str.split('T_')
        if len(parts) == 2 and len(parts[1]) == 4:
            hour = parts[1][:2]
            minute = parts[1][2:]
            iso_str = f"{parts[0]}T{hour}:{minute}:00"
            return datetime.fromisoformat(iso_str)
    elif '_' in time_str and 'T' in time_str:
        # Handle format like 2025-08-31T07_00
        if time_str.count('_') == 1 and time_str.endswith('_00'):
            base_part = time_str[:-3]  # Remove _00
            if 'T' in base_part:
                iso_str = f"{base_part}:00:00"
                return datetime.fromisoformat(iso_str)
    return datetime.fromisoformat(time_str)

# Optional dependencies with graceful degradation
try:
    from scipy.spatial import KDTree
    SCIPY_AVAILABLE = True
except ImportError:
    SCIPY_AVAILABLE = False

try:
    import cfgrib
    CFGRIB_AVAILABLE = True
except ImportError:
    CFGRIB_AVAILABLE = False

# Import validation models
try:
    from weather_models import WeatherForecastValidator, WeatherForecast
    VALIDATION_AVAILABLE = True
except ImportError:
    VALIDATION_AVAILABLE = False

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRATISLAVA_COORDS = {'lat': 48.185872101456816, 'lon': 17.1850614008809}
VARIABLES = {
    'TOT_PREC': {
        'name': 'Precipitation',
        'unit': 'mm/h',
        'base_url': 'https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/',
        'accumulated': True
    },
    'VMAX_10M': {
        'name': 'Wind Gust',
        'unit': 'm/s',
        'base_url': 'https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/VMAX_10M/r/',
        'accumulated': False
    }
}

# Grid configuration
GRID_URL = "https://opendata.dwd.de/weather/lib/cdo/icon_grid_0047_R19B07_L.nc.bz2"
GRID_FILE = "data/grid/icon_grid_0047_R19B07_L.nc"

# Global session with connection pooling
_session = None

# Simple cache for API responses
_cache = {}
_cache_ttl = {}
CACHE_TTL_SECONDS = 300  # 5 minutes

def get_cached_response(key: str):
    """Get cached response if it exists and hasn't expired"""
    if key not in _cache:
        return None
    
    if time.time() - _cache_ttl.get(key, 0) > CACHE_TTL_SECONDS:
        # Cache expired
        _cache.pop(key, None)
        _cache_ttl.pop(key, None)
        return None
    
    return _cache[key]

def set_cached_response(key: str, value):
    """Cache a response with current timestamp"""
    _cache[key] = value
    _cache_ttl[key] = time.time()

def get_session():
    """Get or create a requests session with connection pooling and retries"""
    global _session
    if _session is None:
        _session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],  # Updated parameter name
            backoff_factor=1
        )
        
        # Configure adapters with connection pooling
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=20,
            pool_maxsize=20
        )
        
        _session.mount("http://", adapter)
        _session.mount("https://", adapter)
        
        # Note: timeout will be set per request
    
    return _session

def discover_available_runs() -> List[Dict]:
    """Discover available forecast runs from DWD"""
    logger.info("🔍 Discovering available forecast runs...")
    
    base_url = VARIABLES['TOT_PREC']['base_url']
    cache_key = f"discover_runs_{base_url}"
    
    # Try to get from cache first
    cached_runs = get_cached_response(cache_key)
    if cached_runs is not None:
        logger.info(f"✅ Using cached run list ({len(cached_runs)} runs)")
        return cached_runs
    
    runs = []
    
    try:
        session = get_session()
        response = session.get(base_url, timeout=10)
        response.raise_for_status()
        
        # Parse HTML to find run directories
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            # Look for format: YYYY-MM-DDTHH%3A00/ (URL-encoded YYYY-MM-DDTHH:00/)
            if href.endswith('/') and '%3A00' in href:
                try:
                    # Remove trailing slash and decode URL
                    import urllib.parse
                    run_str_decoded = urllib.parse.unquote(href.rstrip('/'))
                    
                    # Parse ISO format: YYYY-MM-DDTHH:00
                    if run_str_decoded.endswith(':00'):
                        run_dt = parse_timestamp_flexible(run_str_decoded)
                        
                        # Use original URL-encoded version for API calls
                        run_str = href.rstrip('/')
                        
                        runs.append({
                            'run_time': run_dt,
                            'run_str': run_str,  # Keep URL-encoded for API calls
                            'display_name': f"{run_dt.strftime('%Y-%m-%d %H:%M')} UTC",
                            'age_hours': (datetime.utcnow() - run_dt).total_seconds() / 3600
                        })
                except (ValueError, TypeError) as e:
                    logger.debug(f"Skipping invalid run format {href}: {e}")
                    continue
        
        # Sort by run time, newest first
        runs.sort(key=lambda x: x['run_time'], reverse=True)
        latest_runs = runs[:10]  # Return latest 10 runs
        
        # Cache the results
        set_cached_response(cache_key, latest_runs)
        
        logger.info(f"✅ Found {len(runs)} available forecast runs")
        return latest_runs
        
    except Exception as e:
        logger.error(f"❌ Error discovering runs: {e}")
        return []

def download_grid_definition() -> Optional[str]:
    """Download and cache ICON grid definition"""
    grid_path = Path(GRID_FILE)
    grid_path.parent.mkdir(parents=True, exist_ok=True)
    
    if grid_path.exists():
        logger.info("📐 Using cached grid definition")
        return str(grid_path)
    
    try:
        logger.info("📐 Downloading ICON grid definition...")
        session = get_session()
        response = session.get(GRID_URL, stream=True, timeout=30)
        response.raise_for_status()
        
        # Download to temporary file first
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            for chunk in response.iter_content(chunk_size=8192):
                tmp_file.write(chunk)
            tmp_path = tmp_file.name
        
        # Decompress if needed
        if GRID_URL.endswith('.bz2'):
            import bz2
            with bz2.BZ2File(tmp_path, 'rb') as f_in:
                with open(grid_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
            os.unlink(tmp_path)
        else:
            shutil.move(tmp_path, grid_path)
        
        logger.info(f"✅ Grid definition downloaded: {grid_path}")
        return str(grid_path)
        
    except Exception as e:
        logger.error(f"❌ Error downloading grid definition: {e}")
        return None

def find_nearest_grid_point(grid_file: str, target_coords: Dict[str, float]) -> Optional[Tuple[int, float]]:
    """Find nearest ICON grid point to target coordinates"""
    if not SCIPY_AVAILABLE:
        logger.warning("⚠️ scipy not available, using approximate grid point")
        return 0, 0.0
    
    try:
        # Load grid coordinates
        grid_ds = xr.open_dataset(grid_file)
        grid_lats = grid_ds.clat.values * 180.0 / np.pi  # Convert from radians
        grid_lons = grid_ds.clon.values * 180.0 / np.pi
        
        # Create coordinate pairs for KDTree
        grid_points = np.column_stack((grid_lats, grid_lons))
        tree = KDTree(grid_points)
        
        # Find nearest point
        target_point = np.array([[target_coords['lat'], target_coords['lon']]])
        distance, index = tree.query(target_point)
        
        # Convert distance to kilometers (approximate)
        distance_km = distance[0] * 111.0  # Rough conversion
        
        logger.info(f"📍 Nearest grid point: index {index[0]}, distance {distance_km:.1f}km")
        return int(index[0]), distance_km
        
    except Exception as e:
        logger.error(f"❌ Error finding nearest grid point: {e}")
        return 0, 0.0


def download_single_file(url: str, output_path: Path) -> bool:
    """Download a single file with retry logic"""
    max_retries = 3
    
    for attempt in range(max_retries):
        try:
            session = get_session()
            response = session.get(url, timeout=30)
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                f.write(response.content)
            
            return True
            
        except Exception as e:
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
            else:
                logger.warning(f"⚠️ Failed to download {url}: {e}")
                return False
    
    return False

def extract_ensemble_point_data(grib_files: List[str], variable: str, grid_index: int, ensemble_id: str) -> Dict:
    """Extract data for specific grid point from GRIB files"""
    if not CFGRIB_AVAILABLE:
        logger.error("❌ cfgrib not available for GRIB processing")
        return {'times': [], 'values': []}
    
    logger.info(f"📊 Extracting {variable} data from {len(grib_files)} files for ensemble {ensemble_id}")
    
    # Map our variable names to actual GRIB variable names
    grib_variable_map = {
        'VMAX_10M': 'max_i10fg',  # Maximum 10m wind gust
        'TOT_PREC': 'unknown_prec'  # Will be determined dynamically
    }
    
    times = []
    values = []
    actual_variable = None
    
    try:
        for grib_file in sorted(grib_files):
            try:
                ds = xr.open_dataset(grib_file, engine='cfgrib')
                
                # First, determine the actual variable name in the GRIB file
                if actual_variable is None:
                    available_vars = list(ds.data_vars.keys())
                    logger.info(f"🔍 Available variables in GRIB: {available_vars}")
                    
                    if variable in grib_variable_map:
                        # Use mapped variable name
                        candidate = grib_variable_map[variable]
                        if candidate in available_vars:
                            actual_variable = candidate
                        elif variable == 'TOT_PREC':
                            # For precipitation, find the first variable that looks like precipitation
                            prec_vars = [v for v in available_vars if 'tp' in v or 'prec' in v.lower() or 'rain' in v.lower()]
                            if prec_vars:
                                actual_variable = prec_vars[0]
                                logger.info(f"🌧️ Using precipitation variable: {actual_variable}")
                    
                    # Fallback: if exact match doesn't work, try the original variable name
                    if actual_variable is None and variable in available_vars:
                        actual_variable = variable
                    
                    # Last resort: use first available variable
                    if actual_variable is None and available_vars:
                        actual_variable = available_vars[0]
                        logger.warning(f"⚠️ Using fallback variable: {actual_variable}")
                
                # Extract value at specific grid point
                if actual_variable and actual_variable in ds:
                    var_data = ds[actual_variable]
                    
                    # Handle different grid structures
                    if hasattr(var_data, 'values'):
                        values_array = var_data.values
                        
                        # For unstructured grid, use flat indexing
                        if values_array.ndim == 1:
                            if grid_index < len(values_array):
                                value = float(values_array[grid_index])
                            else:
                                logger.warning(f"⚠️ Grid index {grid_index} out of bounds for array size {len(values_array)}")
                                continue
                        else:
                            # For structured grid, use flat indexing
                            value = float(values_array.flat[grid_index])
                        
                        # Skip NaN values
                        if np.isnan(value):
                            logger.warning(f"⚠️ Skipping NaN value in {grib_file}")
                            continue
                        
                        logger.info(f"🎯 Extracted value {value} at grid index {grid_index} from {grib_file}")
                        
                        # Get forecast time
                        if 'time' in ds.coords:
                            forecast_time = ds.time.values
                            if 'step' in ds.coords:
                                forecast_time = forecast_time + ds.step.values
                            times.append(pd.Timestamp(forecast_time).to_pydatetime())
                        else:
                            # Fallback: extract step from filename
                            import re
                            step_match = re.search(r'PT(\d+)H\d+M\.grib2$', grib_file)
                            if step_match:
                                step_hours = int(step_match.group(1))
                                times.append(datetime.utcnow() + pd.Timedelta(hours=step_hours))
                            else:
                                # Use file index as fallback
                                times.append(datetime.utcnow() + pd.Timedelta(hours=len(times)))
                        
                        values.append(value)
                        logger.debug(f"Extracted value {value} from {grib_file}")
                else:
                    logger.warning(f"⚠️ Variable {actual_variable} not found in {grib_file}")
                
                ds.close()
                
            except Exception as e:
                logger.warning(f"⚠️ Error processing {grib_file}: {e}")
                continue
        
        # Handle accumulated variables (like precipitation)
        result = {
            'ensemble_id': ensemble_id,
            'variable': variable,
            'times': times, 
            'values': values,
            'grib_variable': actual_variable
        }
        
        if VARIABLES[variable]['accumulated'] and len(values) > 1:
            # Store original accumulated values and times for accumulated variable
            result['accumulated_values'] = values.copy()
            result['accumulated_times'] = times.copy()  # Store original times for accumulated data
            
            # Vectorized deaccumulation: convert total precipitation to rates
            values_array = np.array(values, dtype=np.float32)
            rates_array = np.diff(values_array, prepend=0.0)  # Vectorized diff with prepended zero
            rates_array = np.maximum(rates_array, 0.0)  # Ensure non-negative values
            # Strip the first value (the prepended 0) and corresponding time
            rates_array = rates_array[1:]  # Remove first element
            
            # Create aligned times for the rates (strip first time to match rates)
            deacc_times = times[1:]  # Remove first time to match rates
            rates = rates_array.tolist()
            result['values'] = rates
            
            # Update times to match the deaccumulated values
            times = deacc_times  # Use the aligned times
            result['times'] = times  # Update result with corrected times
            
            # Calculate 1-hour precipitation sums for specific hour bins
            hourly_values = []
            hourly_times = []
            
            if times and len(times) > 1:
                # Calculate time step in minutes
                time_diff = (times[1] - times[0]).total_seconds() / 60  # minutes
                steps_per_hour = int(60 / time_diff) if time_diff > 0 else 12  # default to 5-min steps
                
                # Group data into hourly bins (e.g., 12:00-13:00, 13:00-14:00)
                if len(times) >= steps_per_hour:
                    # Find the first full hour boundary
                    start_time = times[0]
                    first_hour = start_time.replace(minute=0, second=0, microsecond=0)
                    if start_time.minute > 0 or start_time.second > 0:
                        first_hour += pd.Timedelta(hours=1)
                    
                    # Calculate hourly sums for each complete hour period
                    current_hour = first_hour
                    i = 0
                    
                    while i < len(times):
                        # Find all timestamps within current hour bin [prev_hour, current_hour)
                        prev_hour = current_hour - pd.Timedelta(hours=1)
                        hour_indices = []
                        
                        for j in range(i, len(times)):
                            if prev_hour < times[j] <= current_hour:
                                hour_indices.append(j)
                            elif times[j] > current_hour:
                                break
                        
                        # If we have data for this hour, calculate sum
                        if hour_indices:
                            hour_sum = sum(rates[idx] for idx in hour_indices)
                            hourly_values.append(hour_sum)
                            hourly_times.append(current_hour)  # End of hour period
                            logger.debug(f"📊 Hour {prev_hour.strftime('%H:%M')} to {current_hour.strftime('%H:%M')}: {hour_sum:.3f} mm from {len(hour_indices)} steps")
                            
                            # Move to the next unprocessed timestamp
                            i = max(hour_indices) + 1
                        else:
                            # No data for this hour, move to next hour
                            i += 1
                        
                        current_hour += pd.Timedelta(hours=1)
                        
                        # Safety check to prevent infinite loops
                        if current_hour > times[-1] + pd.Timedelta(hours=2):
                            break
                    
                    logger.info(f"✅ Generated {len(hourly_values)} hourly precipitation bins")
                
                result['hourly_values'] = hourly_values
                result['hourly_times'] = hourly_times
        
        logger.info(f"✅ Extracted {len(values)} {variable} values for ensemble {ensemble_id} using GRIB variable '{actual_variable}'")
        return result
        
    except Exception as e:
        logger.error(f"❌ Error extracting point data: {e}")
        return {'times': [], 'values': []}

def create_derived_variable(run_dir: Path, base_variable: str, data_type: str, name: str, unit: str) -> Dict:
    """Create derived variable from existing ensemble data"""
    logger.info(f"📊 Creating derived variable: {name} ({data_type})")
    
    # Load all ensemble files for the base variable
    ensemble_files = list(run_dir.glob(f"{base_variable}_ensemble_*.json"))
    if not ensemble_files:
        logger.warning(f"⚠️ No ensemble files found for {base_variable}")
        return {}
    
    derived_ensemble_data = []
    
    for ensemble_file in sorted(ensemble_files):
        try:
            with open(ensemble_file, 'r') as f:
                ensemble_data = json.load(f)
            
            # Load the original data with accumulated/hourly values
            original_file = run_dir / f"{base_variable}_ensemble_{ensemble_data['ensemble_id']}.json"
            if not original_file.exists():
                continue
                
            # Extract the derived data type
            derived_data = {'times': [], 'values': []}
            
            if data_type == 'accumulated' and 'accumulated_values' in ensemble_data:
                # Use the original accumulated values before deaccumulation
                if 'accumulated_times' in ensemble_data:
                    # Use proper accumulated times if available
                    derived_data['times'] = [parse_timestamp_flexible(t) for t in ensemble_data['accumulated_times']]
                else:
                    # Fallback: reconstruct times by adding back the first time step
                    regular_times = ensemble_data['times']
                    accumulated_values = ensemble_data['accumulated_values']
                    
                    if len(accumulated_values) == len(regular_times) + 1:
                        # Reconstruct the original first time (which was stripped during deaccumulation)
                        if regular_times:
                            # Parse first regular time and subtract 5 minutes to get the original first time
                            first_regular_time = parse_timestamp_flexible(regular_times[0])
                            first_original_time = first_regular_time - pd.Timedelta(minutes=5)
                            reconstructed_times = [first_original_time.isoformat()] + regular_times
                            derived_data['times'] = [parse_timestamp_flexible(t) for t in reconstructed_times]
                        else:
                            logger.warning(f"⚠️ Cannot reconstruct accumulated times - no regular times available")
                            derived_data['times'] = []
                    else:
                        # Length mismatch - use regular times and hope for the best
                        logger.warning(f"⚠️ Accumulated values length ({len(accumulated_values)}) doesn't match expected pattern")
                        derived_data['times'] = [parse_timestamp_flexible(t) for t in regular_times]
                
                derived_data['values'] = ensemble_data.get('accumulated_values', [])
            elif data_type == 'hourly' and 'hourly_values' in ensemble_data:
                # Use the hourly summed values 
                hourly_times = ensemble_data.get('hourly_times', [])
                hourly_values = ensemble_data.get('hourly_values', [])
                
                # Handle length mismatch by truncating to shorter length
                if len(hourly_times) != len(hourly_values):
                    logger.warning(f"⚠️ Ensemble {ensemble_data.get('ensemble_id', '??')} hourly length mismatch: {len(hourly_values)} values vs {len(hourly_times)} times")
                    min_len = min(len(hourly_values), len(hourly_times))
                    hourly_times = hourly_times[:min_len]
                    hourly_values = hourly_values[:min_len]
                    logger.info(f"✂️ Truncated to {min_len} elements")
                
                derived_data['times'] = [parse_timestamp_flexible(t) for t in hourly_times]
                derived_data['values'] = hourly_values
            
            if derived_data['values']:
                derived_data['ensemble_id'] = ensemble_data['ensemble_id']
                derived_ensemble_data.append(derived_data)
                
        except Exception as e:
            logger.warning(f"⚠️ Error processing ensemble file {ensemble_file}: {e}")
            continue
    
    if not derived_ensemble_data:
        logger.warning(f"⚠️ No valid derived data found for {name}")
        return {}
    
    # Compute statistics for the derived variable
    stats = compute_ensemble_statistics(derived_ensemble_data, f"{base_variable}_{data_type.upper()}")
    
    if stats:
        return {
            'name': name,
            'unit': unit,
            'num_ensembles': stats['num_ensembles'],
            'times': [parse_timestamp_flexible(t).isoformat() if isinstance(t, str) else t.isoformat() for t in stats['times']],
            'ensemble_statistics': {
                'tp_mean': stats['statistics']['mean'],
                'tp_median': stats['statistics']['median'],
                'tp_p05': stats['statistics']['p05'],
                'tp_p10': stats['statistics']['p10'],
                'tp_p25': stats['statistics']['p25'],
                'tp_p50': stats['statistics']['p50'],
                'tp_p75': stats['statistics']['p75'],
                'tp_p90': stats['statistics']['p90'],
                'tp_p95': stats['statistics']['p95'],
                'tp_min': stats['statistics']['min'],
                'tp_max': stats['statistics']['max'],
                'tp_std': stats['statistics']['std']
            }
        }
    
    return {}

def compute_ensemble_statistics(ensemble_data: List[Dict], variable: str) -> Dict:
    """Compute ensemble statistics from individual ensemble data using vectorized operations"""
    if not ensemble_data or not ensemble_data[0].get('values'):
        return {}
    
    import numpy as np
    
    # Collect all ensemble values at each time step
    all_values = []
    times = ensemble_data[0]['times']  # Assume all ensembles have same times
    
    for ensemble in ensemble_data:
        if len(ensemble['values']) == len(times):
            all_values.append(ensemble['values'])
        else:
            logger.warning(f"⚠️ Ensemble {ensemble.get('ensemble_id', 'unknown')} has different time length")
    
    if not all_values:
        return {}
    
    # Convert to numpy array for statistics (ensembles x time_steps)
    values_array = np.array(all_values, dtype=np.float32)  # Use float32 for memory efficiency
    
    # Compute all percentiles in one call for better performance
    percentiles = [5, 10, 25, 50, 75, 90, 95]
    percentile_values = np.percentile(values_array, percentiles, axis=0)
    
    # Compute other statistics using vectorized operations
    stats = {
        'mean': np.mean(values_array, axis=0).tolist(),
        'median': percentile_values[3].tolist(),  # 50th percentile is median
        'p05': percentile_values[0].tolist(),
        'p10': percentile_values[1].tolist(),
        'p25': percentile_values[2].tolist(),
        'p50': percentile_values[3].tolist(),
        'p75': percentile_values[4].tolist(),
        'p90': percentile_values[5].tolist(),
        'p95': percentile_values[6].tolist(),
        'min': np.min(values_array, axis=0).tolist(),
        'max': np.max(values_array, axis=0).tolist(),
        'std': np.std(values_array, axis=0, dtype=np.float32).tolist()
    }
    
    logger.info(f"📊 Computed ensemble statistics for {variable} from {len(all_values)} ensembles")
    
    return {
        'times': times,
        'num_ensembles': len(all_values),
        'statistics': stats
    }

def process_forecast_run(run_str: str, output_dir: Path, location_coords: Dict[str, float]) -> Optional[str]:
    """Process a complete forecast run for both variables with incremental processing"""
    logger.info(f"🚀 Processing forecast run: {run_str}")
    
    # Download grid definition
    grid_file = download_grid_definition()
    if not grid_file:
        return None
    
    # Find nearest grid point
    grid_index, distance_km = find_nearest_grid_point(grid_file, location_coords)
    
    # Process both variables
    result_data = {
        'run_time': run_str,
        'location': 'Bratislava',
        'coordinates': [location_coords['lat'], location_coords['lon']],
        'grid_distance_km': distance_km,
        'processed_at': datetime.now(timezone.utc).isoformat(),
        'variables': {}
    }
    
    # Create run-specific directory
    run_dir = output_dir / f"forecast_{run_str}"
    run_dir.mkdir(parents=True, exist_ok=True)
    
    for variable in ['TOT_PREC', 'VMAX_10M']:
        logger.info(f"📈 Processing {variable}...")
        
        # Process ensembles in bulk
        ensemble_stats = process_variable_bulk(
            run_str, variable, grid_index, distance_km, location_coords, run_dir
        )
        
        if ensemble_stats:
            result_data['variables'][variable] = ensemble_stats
    
    # For TOT_PREC, also create derived variables from existing ensemble data
    if 'TOT_PREC' in result_data['variables']:
        logger.info("📈 Creating derived precipitation variables...")
        
        # Create accumulated precipitation variable
        result_data['variables']['TOT_PREC_ACCUM'] = create_derived_variable(
            run_dir, 'TOT_PREC', 'accumulated', 
            name='Accumulated Precipitation',
            unit='mm'
        )
        
        # Create 1-hour precipitation variable  
        result_data['variables']['TOT_PREC_1H'] = create_derived_variable(
            run_dir, 'TOT_PREC', 'hourly',
            name='1-Hour Precipitation', 
            unit='mm'
        )
    
    # Save summary JSON
    output_file = output_dir / f"forecast_{run_str}.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        # Validate JSON structure before saving
        if VALIDATION_AVAILABLE:
            try:
                WeatherForecastValidator.validate_json_data(result_data)
                logger.info("✅ JSON data validation passed")
            except Exception as e:
                logger.warning(f"❌ JSON validation failed: {e}")
                logger.warning("⚠️ Continuing with potentially invalid data...")
        
        with open(output_file, 'w') as f:
            json.dump(result_data, f, indent=2, default=str)
        
        # Validate saved file as final check
        if VALIDATION_AVAILABLE:
            try:
                WeatherForecastValidator.validate_json_file(output_file)
                logger.info("✅ Saved file validation passed")
            except Exception as e:
                logger.warning(f"❌ Saved file validation failed: {e}")
        
        logger.info(f"✅ Saved forecast summary: {output_file}")
        return str(output_file)
        
    except Exception as e:
        logger.error(f"❌ Error saving JSON: {e}")
        return None

def process_variable_bulk(run_str: str, variable: str, grid_index: int, 
                         distance_km: float, location_coords: Dict[str, float], 
                         run_dir: Path) -> Optional[Dict]:
    """Process a single variable by downloading all ensembles in bulk"""
    logger.info(f"📊 Starting bulk processing for {variable}")
    
    # Get ensemble list first
    base_url = VARIABLES[variable]['base_url']
    ensemble_url = f"{base_url}{run_str}/e/"
    
    try:
        session = get_session()
        response = session.get(ensemble_url, timeout=10)
        if response.status_code != 200:
            logger.error(f"❌ Cannot access ensemble directory for {run_str}")
            return None
        
        # Parse HTML to find ALL available ensembles
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        
        ensembles = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('/') and href[:-1].isdigit():
                ensembles.append(href[:-1])  # Remove trailing slash
        
        if not ensembles:
            logger.error(f"❌ No ensemble members found for {run_str}")
            return None
        
        ensembles.sort()
        logger.info(f"📊 Found {len(ensembles)} ensemble members: {ensembles}")
        
        # Download all ensemble files first
        all_ensemble_files = {}
        for ensemble_id in ensembles:
            logger.info(f"📥 Downloading ensemble {ensemble_id}/{len(ensembles)}")
            ensemble_files = download_single_ensemble(run_str, variable, ensemble_id)
            if ensemble_files:
                all_ensemble_files[ensemble_id] = ensemble_files
            else:
                logger.warning(f"⚠️ No files downloaded for ensemble {ensemble_id}")
        
        # Process all ensembles in bulk
        ensemble_data = []
        for ensemble_id, ensemble_files in all_ensemble_files.items():
            logger.info(f"📊 Processing ensemble {ensemble_id} data")
            
            # Extract point data for this ensemble
            point_data = extract_ensemble_point_data(ensemble_files, variable, grid_index, ensemble_id)
            
            if point_data.get('values'):
                ensemble_data.append(point_data)
                logger.debug(f"✅ Processed ensemble {ensemble_id}")
            
            # Clean up GRIB files after processing
            for grib_file in ensemble_files:
                try:
                    os.unlink(grib_file)
                    logger.debug(f"🗑️ Cleaned up {grib_file}")
                except Exception as e:
                    logger.debug(f"Warning: Could not delete {grib_file}: {e}")
        
        # Save individual ensemble JSON files after all processing is complete
        if ensemble_data:
            logger.info(f"💾 Saving {len(ensemble_data)} ensemble JSON files")
            for point_data in ensemble_data:
                ensemble_file = run_dir / f"{variable}_ensemble_{point_data['ensemble_id']}.json"
                ensemble_result = {
                    'run_time': run_str,
                    'location': 'Bratislava',
                    'coordinates': [location_coords['lat'], location_coords['lon']],
                    'grid_distance_km': distance_km,
                    'ensemble_id': point_data['ensemble_id'],
                    'variable': variable,
                    'name': VARIABLES[variable]['name'],
                    'unit': VARIABLES[variable]['unit'],
                    'times': [t.isoformat() for t in point_data['times']],
                    'values': point_data['values'],
                    'grib_variable': point_data.get('grib_variable'),
                    'processed_at': datetime.now(timezone.utc).isoformat()
                }
                
                # Add accumulated and hourly data if available
                if 'accumulated_values' in point_data:
                    ensemble_result['accumulated_values'] = point_data['accumulated_values']
                if 'hourly_values' in point_data:
                    ensemble_result['hourly_values'] = point_data['hourly_values']
                    ensemble_result['hourly_times'] = [t.isoformat() for t in point_data['hourly_times']]
                
                with open(ensemble_file, 'w') as f:
                    json.dump(ensemble_result, f, indent=2, default=str)
                logger.debug(f"💾 Saved {ensemble_file}")
        
        # Compute ensemble statistics from all processed ensembles
        if ensemble_data:
            stats = compute_ensemble_statistics(ensemble_data, variable)
            if stats:
                # Use statistics directly without cropping
                # Note: TOT_PREC deaccumulation already handles first timestamp correctly
                return {
                    'name': VARIABLES[variable]['name'],
                    'unit': VARIABLES[variable]['unit'],
                    'num_ensembles': stats['num_ensembles'],
                    'times': [parse_timestamp_flexible(t).isoformat() if isinstance(t, str) else t.isoformat() for t in stats['times']],
                    'ensemble_statistics': {
                        'tp_mean': stats['statistics']['mean'],
                        'tp_median': stats['statistics']['median'],
                        'tp_p05': stats['statistics']['p05'],
                        'tp_p10': stats['statistics']['p10'],
                        'tp_p25': stats['statistics']['p25'],
                        'tp_p50': stats['statistics']['p50'],
                        'tp_p75': stats['statistics']['p75'],
                        'tp_p90': stats['statistics']['p90'],
                        'tp_p95': stats['statistics']['p95'],
                        'tp_min': stats['statistics']['min'],
                        'tp_max': stats['statistics']['max'],
                        'tp_std': stats['statistics']['std']
                    }
                }
        
        logger.warning(f"⚠️ No valid ensemble data found for {variable}")
        return None
        
    except Exception as e:
        logger.error(f"❌ Error in incremental processing for {variable}: {e}")
        return None

def download_single_ensemble(run_str: str, variable: str, ensemble_id: str) -> List[str]:
    """Download GRIB files for a single ensemble member"""
    logger.debug(f"📥 Downloading {variable} ensemble {ensemble_id}")
    
    base_url = VARIABLES[variable]['base_url']
    step_url = f"{base_url}{run_str}/e/{ensemble_id}/s/"
    
    # Create temporary directory for this ensemble
    temp_dir = Path(tempfile.mkdtemp())
    downloaded_files = []
    
    try:
        # Get available forecast steps for this ensemble
        session = get_session()
        response = session.get(step_url, timeout=10)
        if response.status_code != 200:
            logger.warning(f"⚠️ Cannot access step directory for {run_str}/e/{ensemble_id}")
            return []
        
        # Parse available steps
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        available_steps = []
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('.grib2') and href.startswith('PT'):
                available_steps.append(href)
        
        # Filter steps based on variable-specific time resolution
        if variable == 'TOT_PREC':
            # Precipitation: 5-minute intervals, download all steps
            steps_to_download = available_steps
        elif variable == 'VMAX_10M':
            # Wind: 1-hour intervals, filter for hourly steps only
            steps_to_download = [step for step in available_steps if 'H00M.grib2' in step]
        else:
            # Default: use all available steps
            steps_to_download = available_steps
        
        logger.debug(f"📋 Ensemble {ensemble_id}: downloading {len(steps_to_download)} files")
        
        # Download the filtered steps with limited concurrency
        with ThreadPoolExecutor(max_workers=8) as executor:  # Increased for better download performance
            futures = []
            
            for i, step_file in enumerate(steps_to_download):
                file_url = f"{step_url}{step_file}"
                output_file = temp_dir / f"{variable}_{run_str}_{ensemble_id}_{i:03d}.grib2"
                
                future = executor.submit(download_single_file, file_url, output_file)
                futures.append((future, output_file))
            
            # Collect successful downloads
            for future, output_file in futures:
                try:
                    if future.result():
                        downloaded_files.append(str(output_file))
                except Exception as e:
                    logger.warning(f"⚠️ Failed to download {output_file.name}: {e}")
        
        logger.debug(f"✅ Downloaded {len(downloaded_files)} files for ensemble {ensemble_id}")
        return downloaded_files
        
    except Exception as e:
        logger.error(f"❌ Error downloading ensemble {ensemble_id}: {e}")
        return []

# Add pandas import for timestamp handling
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False
    # Fallback timestamp handling
    import pandas as pd
    
def is_run_already_processed(run_str: str, output_dir: str = "data/weather") -> bool:
    """Check if a forecast run has already been processed completely"""
    output_path = Path(output_dir)
    json_file = output_path / f"forecast_{run_str}.json"
    run_dir = output_path / f"forecast_{run_str}"
    
    if not json_file.exists():
        return False
    
    try:
        # Check if the JSON file is complete and has both variables
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        variables = data.get('variables', {})
        
        # Check if we have both required variables
        has_precipitation = 'TOT_PREC' in variables
        has_wind = 'VMAX_10M' in variables
        
        if not (has_precipitation and has_wind):
            logger.warning(f"⚠️ Run {run_str} missing variables (TOT_PREC: {has_precipitation}, VMAX_10M: {has_wind})")
            return False
        
        # Check if precipitation data has reasonable amount of timestamps
        tot_prec = variables['TOT_PREC']
        if 'times' not in tot_prec or len(tot_prec['times']) < 50:  # Should have ~167 timestamps for 5-min data
            logger.warning(f"⚠️ Run {run_str} has insufficient TOT_PREC timestamps: {len(tot_prec.get('times', []))}")
            return False
        
        # Check if wind data has reasonable amount of timestamps
        vmax = variables['VMAX_10M']
        if 'times' not in vmax or len(vmax['times']) < 5:  # Should have ~13 timestamps for hourly data
            logger.warning(f"⚠️ Run {run_str} has insufficient VMAX_10M timestamps: {len(vmax.get('times', []))}")
            return False
        
        # Check if derived precipitation variables exist
        has_accum = 'TOT_PREC_ACCUM' in variables
        has_hourly = 'TOT_PREC_1H' in variables
        
        if not (has_accum and has_hourly):
            logger.warning(f"⚠️ Run {run_str} missing derived variables (ACCUM: {has_accum}, 1H: {has_hourly})")
            return False
        
        # Check if individual ensemble files exist
        if run_dir.exists():
            prec_ensembles = list(run_dir.glob("TOT_PREC_ensemble_*.json"))
            wind_ensembles = list(run_dir.glob("VMAX_10M_ensemble_*.json"))
            
            if len(prec_ensembles) < 10 or len(wind_ensembles) < 10:  # Expect ~20 ensemble members
                logger.warning(f"⚠️ Run {run_str} has insufficient ensembles (TOT_PREC: {len(prec_ensembles)}, VMAX_10M: {len(wind_ensembles)})")
                return False
        else:
            logger.warning(f"⚠️ Run {run_str} missing ensemble directory: {run_dir}")
            return False
        
        # Check if ensemble statistics have reasonable data
        if 'ensemble_statistics' in tot_prec:
            stats = tot_prec['ensemble_statistics']
            if 'tp_mean' not in stats or not stats['tp_mean'] or len(stats['tp_mean']) < 50:
                logger.warning(f"⚠️ Run {run_str} has insufficient TOT_PREC ensemble statistics")
                return False
        
        if 'ensemble_statistics' in vmax:
            stats = vmax['ensemble_statistics']
            if 'tp_mean' not in stats or not stats['tp_mean'] or len(stats['tp_mean']) < 5:
                logger.warning(f"⚠️ Run {run_str} has insufficient VMAX_10M ensemble statistics")
                return False
        
        logger.info(f"✅ Run {run_str} already processed (complete with {len(prec_ensembles)} ensembles)")
        return True
        
    except Exception as e:
        logger.warning(f"⚠️ Error validating existing file for {run_str}: {e}")
        return False

def cleanup_incomplete_run(run_str: str, output_dir: str = "data/weather"):
    """Clean up incomplete or corrupted run files"""
    output_path = Path(output_dir)
    json_file = output_path / f"forecast_{run_str}.json"
    run_dir = output_path / f"forecast_{run_str}"
    
    logger.info(f"🧹 Cleaning up incomplete run: {run_str}")
    
    # Remove main JSON file if it exists
    if json_file.exists():
        try:
            json_file.unlink()
            logger.debug(f"Removed {json_file}")
        except Exception as e:
            logger.warning(f"Could not remove {json_file}: {e}")
    
    # Remove ensemble directory if it exists
    if run_dir.exists():
        try:
            import shutil
            shutil.rmtree(run_dir)
            logger.debug(f"Removed directory {run_dir}")
        except Exception as e:
            logger.warning(f"Could not remove directory {run_dir}: {e}")

def simple_process(run_str: str, output_dir: str = "data/weather", overwrite: bool = False) -> Optional[str]:
    """Simplified entry point for processing a single forecast run"""
    output_path = Path(output_dir)
    
    # Check if already processed (unless overwrite is requested)
    is_complete = is_run_already_processed(run_str, output_dir)
    
    if not overwrite and is_complete:
        logger.info(f"⏭️ Skipping already processed run: {run_str}")
        return str(output_path / f"forecast_{run_str}.json")
    
    if overwrite:
        logger.info(f"🔄 Overwriting existing data for run: {run_str}")
        cleanup_incomplete_run(run_str, output_dir)
    elif not is_complete:
        # Run exists but is incomplete - clean it up before reprocessing
        json_file = output_path / f"forecast_{run_str}.json"
        if json_file.exists():
            logger.info(f"🔧 Detected incomplete run, cleaning up before reprocessing")
            cleanup_incomplete_run(run_str, output_dir)
    
    return process_forecast_run(run_str, output_path, BRATISLAVA_COORDS)

def main():
    """Main entry point with command-line argument parsing"""
    parser = argparse.ArgumentParser(
        description='Process ICON-D2-RUC-EPS forecast data for Bratislava',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python weather_processor.py                    # Process latest 4 runs (skip existing)
  python weather_processor.py --runs 2          # Process latest 2 runs only
  python weather_processor.py --overwrite       # Reprocess all runs, overwriting existing
  python weather_processor.py --run-id 2025-08-30T10:00  # Process specific run
        ''')
    
    parser.add_argument('--runs', type=int, default=4,
                        help='Number of latest runs to process (default: 4)')
    parser.add_argument('--overwrite', action='store_true',
                        help='Overwrite existing processed runs')
    parser.add_argument('--run-id', type=str,
                        help='Process specific run ID (e.g., 2025-08-30T10%%3A00)')
    parser.add_argument('--output-dir', type=str, default='data/weather',
                        help='Output directory for processed data (default: data/weather)')
    
    args = parser.parse_args()
    
    logger.info(f"🚀 Starting weather processor with {'overwrite' if args.overwrite else 'skip existing'} mode")
    
    if args.run_id:
        # Process specific run
        logger.info(f"🎯 Processing specific run: {args.run_id}")
        result = simple_process(args.run_id, args.output_dir, args.overwrite)
        if result:
            logger.info(f"✅ Success: {result}")
        else:
            logger.error("❌ Processing failed")
            return 1
    else:
        # Process multiple latest runs
        logger.info(f"🔍 Discovering available runs...")
        runs = discover_available_runs()
        
        if not runs:
            logger.error("❌ No runs available")
            return 1
        
        selected_runs = runs[:args.runs]
        logger.info(f"📋 Found {len(runs)} available runs, processing latest {len(selected_runs)}:")
        
        successful = 0
        skipped = 0
        failed = 0
        
        for i, run in enumerate(selected_runs):
            logger.info(f"\n{'='*60}")
            logger.info(f"🏃 Run {i+1}/{len(selected_runs)}: {run['display_name']} (age: {run['age_hours']:.1f}h)")
            
            # Check if already processed before calling simple_process
            if not args.overwrite and is_run_already_processed(run['run_str'], args.output_dir):
                skipped += 1
                logger.info(f"⏭️ Skipped existing run")
            else:
                result = simple_process(run['run_str'], args.output_dir, args.overwrite)
                
                if result:
                    successful += 1
                    logger.info(f"✅ Success: {result}")
                else:
                    failed += 1
                    logger.error(f"❌ Failed: {run['run_str']}")
        
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 Processing Summary:")
        logger.info(f"  ✅ Successful: {successful}")
        logger.info(f"  ⏭️ Skipped: {skipped}")
        logger.info(f"  ❌ Failed: {failed}")
        logger.info(f"  📋 Total: {len(selected_runs)}")
        
        if failed > 0:
            return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)