#!/usr/bin/env python3
"""
Test version of Bratislava ICON-D2-RUC-EPS Complete Processing Pipeline
Only processes a small subset for testing
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

import numpy as np
import xarray as xr
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
from tqdm import tqdm
from bs4 import BeautifulSoup
import re

# Configuration
BRATISLAVA_COORDS = {'lat': 48.1486, 'lon': 17.1077}
BASE_DATA_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/"
BASE_ENSEMBLE_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/{run_time}/e/"
BASE_STEP_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/{run_time}/e/{ensemble}/s/"
BASE_DOWNLOAD_URL = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/{run_time}/e/{ensemble}/s/{step}"
OUTPUT_DIR = Path("data/bratislava")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# TEST Configuration - limited for quick testing
CONFIG = {
    'num_runs': 1,                    # Only 1 run for testing
    'max_workers': 4,                 # Limited workers
    'target_unit': 'mm/h',           # Output unit
    'percentiles': [5, 25, 50, 75, 95],  # Fewer percentiles
    'max_ensembles': 2,              # Only first 2 ensembles
    'max_steps': 5,                  # Only first 5 time steps
}

def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime('%H:%M:%S')
    print(f"[{timestamp}] {message}")

def test_basic_functions():
    """Test the basic URL and discovery functions"""
    log("🧪 Testing basic functions...")
    
    # Test discovering runs
    log("Testing run discovery...")
    try:
        response = requests.get(BASE_DATA_URL, timeout=10)
        response.raise_for_status()
        log("✅ Base URL accessible")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        run_time_pattern = r'(\d{4}-\d{2}-\d{2}T\d{2}%3A00)/'
        run_times = []
        
        for link in soup.find_all('a'):
            href = link.get('href', '')
            match = re.search(run_time_pattern, href)
            if match:
                run_time_str = match.group(1)
                run_times.append(run_time_str)
        
        if run_times:
            log(f"✅ Found {len(run_times)} runs")
            latest_run = run_times[-1]  # Get the latest
            log(f"Latest run: {latest_run.replace('%3A', ':')}")
            return latest_run
        else:
            log("❌ No runs found")
            return None
            
    except Exception as e:
        log(f"❌ Error testing discovery: {e}")
        return None

def test_ensemble_discovery(run_time_str):
    """Test ensemble discovery for a run"""
    log(f"🧪 Testing ensemble discovery for {run_time_str}...")
    
    ensemble_url = BASE_ENSEMBLE_URL.format(run_time=run_time_str)
    
    try:
        response = requests.get(ensemble_url, timeout=10)
        response.raise_for_status()
        log("✅ Ensemble URL accessible")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        ensembles = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('/') and href[:-1].isdigit():
                ensemble_num = href[:-1]
                ensembles.append(ensemble_num)
        
        ensembles.sort(key=int)
        if ensembles:
            log(f"✅ Found {len(ensembles)} ensembles: {ensembles[:5]}...")
            return ensembles[:CONFIG['max_ensembles']]
        else:
            log("❌ No ensembles found")
            return []
            
    except Exception as e:
        log(f"❌ Error testing ensemble discovery: {e}")
        return []

def test_step_discovery(run_time_str, ensemble):
    """Test step discovery for a run and ensemble"""
    log(f"🧪 Testing step discovery for run {run_time_str}, ensemble {ensemble}...")
    
    step_url = BASE_STEP_URL.format(run_time=run_time_str, ensemble=ensemble)
    
    try:
        response = requests.get(step_url, timeout=10)
        response.raise_for_status()
        log("✅ Step URL accessible")
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        steps = []
        for link in soup.find_all('a'):
            href = link.get('href', '')
            if href.endswith('.grib2') and 'PT' in href:
                steps.append(href)
        
        steps.sort()
        if steps:
            log(f"✅ Found {len(steps)} steps: {steps[:3]}...")
            return steps[:CONFIG['max_steps']]
        else:
            log("❌ No steps found")
            return []
            
    except Exception as e:
        log(f"❌ Error testing step discovery: {e}")
        return []

def test_download(run_time_str, ensemble, step):
    """Test downloading a single file"""
    log(f"🧪 Testing download for {step}...")
    
    url = BASE_DOWNLOAD_URL.format(run_time=run_time_str, ensemble=ensemble, step=step)
    
    # Create filename
    readable_time = run_time_str.replace('%3A', ':')
    run_time_dt = datetime.strptime(readable_time, '%Y-%m-%dT%H:%M')
    filename = f"test_icon_d2_ruc_eps_TOT_PREC_{run_time_dt.strftime('%Y%m%d')}_{run_time_dt.strftime('%H')}_e{ensemble}_{step}"
    filepath = OUTPUT_DIR / "test" / filename
    filepath.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        file_size = filepath.stat().st_size / 1024  # KB
        log(f"✅ Downloaded {filename} ({file_size:.1f} KB)")
        return str(filepath)
        
    except Exception as e:
        log(f"❌ Error downloading {step}: {e}")
        return None

def test_point_extraction(filepath):
    """Test extracting Bratislava point from a GRIB file"""
    log(f"🧪 Testing point extraction from {Path(filepath).name}...")
    
    try:
        # Try to open with cfgrib
        ds = xr.open_dataset(filepath, engine='cfgrib')
        log(f"✅ Opened GRIB file with cfgrib")
        
        # Check available variables
        log(f"Available variables: {list(ds.data_vars.keys())}")
        log(f"Coordinates: {list(ds.coords.keys())}")
        log(f"Dimensions: {dict(ds.dims)}")
        
        # Check if we have lat/lon
        if 'latitude' in ds.coords and 'longitude' in ds.coords:
            log("✅ Found latitude/longitude coordinates")
            
            # Find nearest point to Bratislava
            lat_diff = np.abs(ds.latitude - BRATISLAVA_COORDS['lat'])
            lon_diff = np.abs(ds.longitude - BRATISLAVA_COORDS['lon'])
            
            # Use combined distance
            distance = np.sqrt(lat_diff**2 + lon_diff**2)
            min_idx = np.unravel_index(np.argmin(distance.values), distance.shape)
            
            # Extract point data
            point_data = ds.isel(y=min_idx[0], x=min_idx[1])
            
            actual_lat = float(ds.latitude[min_idx])
            actual_lon = float(ds.longitude[min_idx])
            
            log(f"✅ Found nearest point: {actual_lat:.4f}°N, {actual_lon:.4f}°E")
            log(f"Distance from target: {np.sqrt((actual_lat - BRATISLAVA_COORDS['lat'])**2 + (actual_lon - BRATISLAVA_COORDS['lon'])**2):.4f}°")
            
            # Check precipitation value
            if 'tp' in point_data.data_vars:
                tp_value = float(point_data.tp.values)
                log(f"✅ Precipitation value: {tp_value:.6f} kg/m²")
            
            ds.close()
            return True
        else:
            log("❌ No latitude/longitude coordinates found")
            ds.close()
            return False
            
    except ImportError:
        log("❌ cfgrib not installed. Install with: pip install cfgrib")
        return False
    except Exception as e:
        log(f"❌ Error extracting point: {e}")
        return False

def main():
    """Main test execution"""
    log("🧪 Starting Bratislava Pipeline Test")
    log(f"   Target location: {BRATISLAVA_COORDS['lat']:.4f}°N, {BRATISLAVA_COORDS['lon']:.4f}°E")
    log(f"   Test configuration: {CONFIG['max_ensembles']} ensembles, {CONFIG['max_steps']} steps")
    
    # Test 1: Basic discovery
    run_time_str = test_basic_functions()
    if not run_time_str:
        log("❌ Basic discovery failed")
        return False
    
    # Test 2: Ensemble discovery
    ensembles = test_ensemble_discovery(run_time_str)
    if not ensembles:
        log("❌ Ensemble discovery failed")
        return False
    
    # Test 3: Step discovery
    steps = test_step_discovery(run_time_str, ensembles[0])
    if not steps:
        log("❌ Step discovery failed")
        return False
    
    # Test 4: Download one file
    filepath = test_download(run_time_str, ensembles[0], steps[0])
    if not filepath:
        log("❌ Download test failed")
        return False
    
    # Test 5: Point extraction
    if not test_point_extraction(filepath):
        log("❌ Point extraction test failed")
        return False
    
    log("🎉 All tests passed! The pipeline should work correctly.")
    log("✅ Ready to run full bratislava_pipeline.py")
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)