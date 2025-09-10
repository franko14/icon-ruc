#!/usr/bin/env python3
"""
Create Ensemble Statistics for QGIS
===================================

Downloads the latest available forecast step for ALL ensemble members from 
ICON-D2-RUC-EPS, calculates ensemble statistics (5%, mean, median, 95%), 
and saves each statistic as a separate NetCDF file for QGIS visualization.

This provides uncertainty visualization by showing the spread of the ensemble.
"""

import os
import sys
import numpy as np
import xarray as xr
import requests
import tempfile
from pathlib import Path
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Optional dependencies with graceful degradation
try:
    import cfgrib
    CFGRIB_AVAILABLE = True
except ImportError:
    logger.error("cfgrib not available. Install with: pip install cfgrib")
    CFGRIB_AVAILABLE = False

try:
    from bs4 import BeautifulSoup
    BS4_AVAILABLE = True
except ImportError:
    logger.error("BeautifulSoup not available. Install with: pip install beautifulsoup4")
    BS4_AVAILABLE = False

try:
    from scipy.interpolate import griddata
    SCIPY_AVAILABLE = True
except ImportError:
    logger.error("scipy not available. Install with: pip install scipy")
    SCIPY_AVAILABLE = False

# Configuration
GRID_URL = "https://opendata.dwd.de/weather/lib/cdo/icon_grid_0047_R19B07_L.nc.bz2"
GRID_FILE = "data/grid/icon_grid_0047_R19B07_L.nc"

# Output grid configuration
OUTPUT_RESOLUTION = 0.02  # degrees (about 2.2 km)
OUTPUT_BOUNDS = {
    'lat_min': 47.0,
    'lat_max': 55.0,
    'lon_min': 5.0,
    'lon_max': 20.0
}

# Ensemble configuration
ENSEMBLE_MEMBERS = list(range(1, 21))  # 1-20 (DWD uses 01-20, not 00-19)
MAX_WORKERS = 5  # Concurrent downloads
FORECAST_STEP = "PT014H00M"  # 14-hour forecast step (should be available)

def download_grid_definition():
    """Download ICON grid definition if needed"""
    grid_path = Path(GRID_FILE)
    grid_path.parent.mkdir(parents=True, exist_ok=True)
    
    if grid_path.exists():
        logger.info("📐 Using cached grid definition")
        return str(grid_path)
    
    try:
        logger.info("📐 Downloading ICON grid definition...")
        response = requests.get(GRID_URL, stream=True, timeout=60)
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
                    import shutil
                    shutil.copyfileobj(f_in, f_out)
            os.unlink(tmp_path)
        else:
            import shutil
            shutil.move(tmp_path, grid_path)
        
        logger.info(f"✅ Grid definition downloaded: {grid_path}")
        return str(grid_path)
        
    except Exception as e:
        logger.error(f"❌ Error downloading grid definition: {e}")
        return None

def find_latest_run():
    """Find the latest available run with the desired forecast step"""
    logger.info("🔍 Finding latest run with available forecast step...")
    
    base_url = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/"
    
    try:
        response = requests.get(base_url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        runs = []
        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.endswith('/') and '%3A00' in href:
                runs.append(href.rstrip('/'))
        
        if not runs:
            raise Exception("No forecast runs found")
        
        # Try runs from latest to oldest
        sorted_runs = sorted(runs, reverse=True)
        logger.info(f"📅 Found {len(sorted_runs)} runs")
        
        # Try each run to see if our forecast step is available
        for run_id in sorted_runs[:10]:
            logger.info(f"🔍 Checking run: {run_id}")
            
            # Test with first ensemble member
            test_url = f"{base_url}{run_id}/e/01/s/{FORECAST_STEP}.grib2"
            
            try:
                response = requests.head(test_url, timeout=10)
                if response.status_code == 200:
                    logger.info(f"✅ Found usable run: {run_id}")
                    return run_id
                else:
                    logger.info(f"   ⚠️ Step {FORECAST_STEP} not available for {run_id}")
            except Exception as e:
                logger.info(f"   ⚠️ Error checking {run_id}: {e}")
                continue
        
        raise Exception(f"No runs found with forecast step {FORECAST_STEP}")
        
    except Exception as e:
        logger.error(f"❌ Error finding latest run: {e}")
        return None

def download_ensemble_member(run_id: str, ensemble_id: int, temp_dir: Path) -> tuple:
    """Download a single ensemble member"""
    base_url = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/"
    
    # Format ensemble ID with leading zero
    ensemble_str = f"{ensemble_id:02d}"
    file_url = f"{base_url}{run_id}/e/{ensemble_str}/s/{FORECAST_STEP}.grib2"
    
    try:
        logger.info(f"📥 Downloading ensemble {ensemble_str}...")
        
        response = requests.get(file_url, timeout=120)
        response.raise_for_status()
        
        # Save to temporary file
        temp_file = temp_dir / f"ensemble_{ensemble_str}.grib2"
        with open(temp_file, 'wb') as f:
            f.write(response.content)
        
        file_size = temp_file.stat().st_size / 1024 / 1024
        logger.info(f"✅ Downloaded ensemble {ensemble_str}: {file_size:.1f} MB")
        
        return ensemble_id, str(temp_file), True
        
    except Exception as e:
        logger.error(f"❌ Error downloading ensemble {ensemble_str}: {e}")
        return ensemble_id, None, False

def process_grib_file(grib_file: str, grid_lats: np.ndarray, grid_lons: np.ndarray) -> tuple:
    """Process a single GRIB file and extract precipitation data"""
    try:
        # Open GRIB file
        ds = xr.open_dataset(grib_file, engine='cfgrib')
        
        # Find precipitation variable
        var_names = list(ds.data_vars.keys())
        prec_var = None
        for var in ['tp', 'unknown', 'precipitation', 'tot_prec']:
            if var in var_names:
                prec_var = var
                break
        
        if prec_var is None and var_names:
            prec_var = var_names[0]
        
        if prec_var is None:
            raise Exception("No variables found in GRIB file")
        
        # Get the data
        data = ds[prec_var].values
        
        if data.ndim > 1:
            data = data.flatten()
        
        # Get metadata
        metadata = {
            'variable_name': prec_var,
            'long_name': ds[prec_var].attrs.get('long_name', 'Total Precipitation'),
            'units': ds[prec_var].attrs.get('units', 'kg m**-2'),
            'time': ds.time.values if 'time' in ds.coords else np.datetime64('now'),
            'step': ds.step.values if 'step' in ds.coords else np.timedelta64(14, 'h')
        }
        
        ds.close()
        
        # Regrid to regular lat/lon
        regridded_data = regrid_data(grid_lats, grid_lons, data)
        
        return regridded_data, metadata, True
        
    except Exception as e:
        logger.error(f"❌ Error processing GRIB file: {e}")
        return None, None, False

def regrid_data(grid_lats: np.ndarray, grid_lons: np.ndarray, data: np.ndarray) -> np.ndarray:
    """Regrid data from unstructured grid to regular lat/lon"""
    # Create output grid
    lat_out = np.arange(OUTPUT_BOUNDS['lat_min'], OUTPUT_BOUNDS['lat_max'], OUTPUT_RESOLUTION)
    lon_out = np.arange(OUTPUT_BOUNDS['lon_min'], OUTPUT_BOUNDS['lon_max'], OUTPUT_RESOLUTION)
    lon_grid, lat_grid = np.meshgrid(lon_out, lat_out)
    
    # Filter source points to reasonable bounds
    mask = ((grid_lats >= OUTPUT_BOUNDS['lat_min'] - 1) & 
            (grid_lats <= OUTPUT_BOUNDS['lat_max'] + 1) &
            (grid_lons >= OUTPUT_BOUNDS['lon_min'] - 1) & 
            (grid_lons <= OUTPUT_BOUNDS['lon_max'] + 1) &
            np.isfinite(data))
    
    grid_lats_filtered = grid_lats[mask]
    grid_lons_filtered = grid_lons[mask]
    data_filtered = data[mask]
    
    if len(data_filtered) < 10:
        raise Exception("Too few valid data points for interpolation")
    
    # Interpolate using scipy.interpolate.griddata
    points = np.column_stack((grid_lats_filtered, grid_lons_filtered))
    grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
    
    # Use linear interpolation with nearest neighbor fallback
    interpolated = griddata(
        points, data_filtered, grid_points, 
        method='linear', fill_value=np.nan
    )
    
    # Fill NaN values with nearest neighbor
    nan_mask = np.isnan(interpolated)
    if np.any(nan_mask):
        nearest = griddata(
            points, data_filtered, grid_points, 
            method='nearest', fill_value=0
        )
        interpolated[nan_mask] = nearest[nan_mask]
    
    # Reshape to grid
    interpolated = interpolated.reshape(lat_grid.shape)
    
    return interpolated

def load_icon_grid():
    """Load ICON grid coordinates"""
    logger.info("📐 Loading ICON grid coordinates...")
    
    try:
        grid_ds = xr.open_dataset(GRID_FILE)
        
        # Convert from radians to degrees
        lats = grid_ds.clat.values * 180.0 / np.pi
        lons = grid_ds.clon.values * 180.0 / np.pi
        
        logger.info(f"✅ Loaded grid: {len(lats)} points")
        logger.info(f"   Lat range: {lats.min():.3f} to {lats.max():.3f}°")
        logger.info(f"   Lon range: {lons.min():.3f} to {lons.max():.3f}°")
        
        grid_ds.close()
        return lats, lons
        
    except Exception as e:
        logger.error(f"❌ Error loading grid: {e}")
        return None, None

def calculate_ensemble_statistics(ensemble_data: list) -> dict:
    """Calculate ensemble statistics"""
    logger.info("📊 Calculating ensemble statistics...")
    
    # Stack all ensemble data
    ensemble_array = np.stack(ensemble_data, axis=0)  # (n_ensembles, lat, lon)
    
    statistics = {
        'mean': np.mean(ensemble_array, axis=0),
        'median': np.median(ensemble_array, axis=0),
        'p05': np.percentile(ensemble_array, 5, axis=0),
        'p95': np.percentile(ensemble_array, 95, axis=0),
        'std': np.std(ensemble_array, axis=0),
        'min': np.min(ensemble_array, axis=0),
        'max': np.max(ensemble_array, axis=0)
    }
    
    logger.info(f"✅ Statistics calculated for {len(ensemble_data)} ensembles")
    
    for stat_name, stat_data in statistics.items():
        logger.info(f"   {stat_name}: {stat_data.min():.3f} to {stat_data.max():.3f}")
    
    return statistics

def save_statistic_netcdf(lat: np.ndarray, lon: np.ndarray, data: np.ndarray, 
                         stat_name: str, metadata: dict, run_id: str, output_dir: Path):
    """Save a single statistic as NetCDF file"""
    
    # Create descriptive names
    stat_descriptions = {
        'mean': 'Ensemble Mean',
        'median': 'Ensemble Median', 
        'p05': '5th Percentile',
        'p95': '95th Percentile',
        'std': 'Standard Deviation',
        'min': 'Ensemble Minimum',
        'max': 'Ensemble Maximum'
    }
    
    description = stat_descriptions.get(stat_name, stat_name)
    filename = f"icon_d2_precipitation_{stat_name}_{run_id.replace('%3A', '_').replace('T', '_')}.nc"
    output_file = output_dir / filename
    
    try:
        # Create xarray dataset
        ds = xr.Dataset(
            {
                'precipitation': (['lat', 'lon'], data.astype(np.float32), {
                    'long_name': f'{description} - {metadata["long_name"]}',
                    'units': metadata['units'],
                    'grid_mapping': 'crs',
                    'ensemble_statistic': stat_name,
                    'ensemble_size': len(ENSEMBLE_MEMBERS)
                })
            },
            coords={
                'lat': (['lat'], lat.astype(np.float32), {
                    'long_name': 'Latitude',
                    'units': 'degrees_north',
                    'axis': 'Y'
                }),
                'lon': (['lon'], lon.astype(np.float32), {
                    'long_name': 'Longitude',
                    'units': 'degrees_east',
                    'axis': 'X'
                }),
                'time': metadata['time'],
                'forecast_step': metadata['step']
            }
        )
        
        # Add CRS information for QGIS
        ds = ds.assign_coords(
            crs=xr.DataArray(
                0,  # dummy variable
                attrs={
                    'grid_mapping_name': 'latitude_longitude',
                    'longitude_of_prime_meridian': 0.0,
                    'semi_major_axis': 6378137.0,
                    'inverse_flattening': 298.257223563,
                    'spatial_ref': 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]',
                    'crs_wkt': 'GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563,AUTHORITY["EPSG","7030"]],AUTHORITY["EPSG","6326"]],PRIMEM["Greenwich",0,AUTHORITY["EPSG","8901"]],UNIT["degree",0.0174532925199433,AUTHORITY["EPSG","9122"]],AUTHORITY["EPSG","4326"]]'
                }
            )
        )
        
        # Add global attributes
        ds.attrs = {
            'title': f'ICON-D2-RUC-EPS Ensemble {description}',
            'description': f'{description} of precipitation from {len(ENSEMBLE_MEMBERS)}-member ensemble',
            'source': 'DWD ICON-D2-RUC-EPS',
            'institution': 'Deutscher Wetterdienst (DWD)',
            'conventions': 'CF-1.8',
            'created': datetime.now().isoformat(),
            'forecast_run': run_id,
            'forecast_step': FORECAST_STEP,
            'ensemble_members': len(ENSEMBLE_MEMBERS),
            'grid_resolution_degrees': OUTPUT_RESOLUTION,
            'original_grid': 'ICON unstructured triangular grid',
            'interpolation_method': 'linear with nearest neighbor fallback',
            'crs': 'EPSG:4326',
            'domain': 'Central Europe',
            'ensemble_statistic': stat_name
        }
        
        # Save to NetCDF
        encoding = {
            'precipitation': {
                'zlib': True,
                'complevel': 4,
                'dtype': 'float32'
            },
            'lat': {'dtype': 'float32'},
            'lon': {'dtype': 'float32'}
        }
        
        ds.to_netcdf(output_file, encoding=encoding)
        ds.close()
        
        file_size = output_file.stat().st_size / 1024 / 1024
        logger.info(f"✅ Saved {stat_name}: {output_file.name} ({file_size:.1f} MB)")
        
        return str(output_file)
        
    except Exception as e:
        logger.error(f"❌ Error saving {stat_name}: {e}")
        return None

def main():
    """Main function"""
    logger.info("🌧️  Creating Ensemble Statistics for QGIS")
    logger.info("=" * 60)
    
    # Check dependencies
    if not CFGRIB_AVAILABLE or not SCIPY_AVAILABLE or not BS4_AVAILABLE:
        logger.error("❌ Missing required dependencies")
        logger.info("Install missing dependencies with:")
        if not CFGRIB_AVAILABLE:
            logger.info("  pip install cfgrib")
        if not SCIPY_AVAILABLE:
            logger.info("  pip install scipy")
        if not BS4_AVAILABLE:
            logger.info("  pip install beautifulsoup4")
        return 1
    
    # Download grid definition
    grid_file = download_grid_definition()
    if not grid_file:
        return 1
    
    # Load ICON grid
    grid_lats, grid_lons = load_icon_grid()
    if grid_lats is None:
        return 1
    
    # Find latest run
    run_id = find_latest_run()
    if not run_id:
        return 1
    
    # Create temporary directory for downloads
    temp_dir = Path(tempfile.mkdtemp())
    logger.info(f"📁 Using temporary directory: {temp_dir}")
    
    try:
        # Download all ensemble members in parallel
        logger.info(f"📥 Downloading {len(ENSEMBLE_MEMBERS)} ensemble members...")
        start_time = time.time()
        
        successful_downloads = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all download tasks
            future_to_ensemble = {
                executor.submit(download_ensemble_member, run_id, ens_id, temp_dir): ens_id
                for ens_id in ENSEMBLE_MEMBERS
            }
            
            # Process completed downloads
            for future in as_completed(future_to_ensemble):
                ensemble_id, file_path, success = future.result()
                if success:
                    successful_downloads.append((ensemble_id, file_path))
        
        download_time = time.time() - start_time
        logger.info(f"✅ Downloaded {len(successful_downloads)}/{len(ENSEMBLE_MEMBERS)} ensembles in {download_time:.1f}s")
        
        if len(successful_downloads) < 5:
            logger.error("❌ Too few successful downloads for meaningful statistics")
            return 1
        
        # Process all GRIB files and regrid
        logger.info("🔄 Processing and regridding ensemble data...")
        start_time = time.time()
        
        ensemble_data = []
        metadata = None
        
        for ensemble_id, file_path in successful_downloads:
            logger.info(f"🌐 Processing ensemble {ensemble_id:02d}...")
            regridded_data, file_metadata, success = process_grib_file(file_path, grid_lats, grid_lons)
            
            if success:
                ensemble_data.append(regridded_data)
                if metadata is None:
                    metadata = file_metadata
        
        process_time = time.time() - start_time
        logger.info(f"✅ Processed {len(ensemble_data)} ensembles in {process_time:.1f}s")
        
        if len(ensemble_data) < 5:
            logger.error("❌ Too few processed ensembles for meaningful statistics")
            return 1
        
        # Calculate ensemble statistics
        statistics = calculate_ensemble_statistics(ensemble_data)
        
        # Create output directory
        output_dir = Path("data/qgis_samples")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Create output grid coordinates
        lat_out = np.arange(OUTPUT_BOUNDS['lat_min'], OUTPUT_BOUNDS['lat_max'], OUTPUT_RESOLUTION)
        lon_out = np.arange(OUTPUT_BOUNDS['lon_min'], OUTPUT_BOUNDS['lon_max'], OUTPUT_RESOLUTION)
        
        # Save each statistic as separate NetCDF file
        logger.info("💾 Saving ensemble statistics as NetCDF files...")
        saved_files = []
        
        # Save key statistics that are useful for QGIS visualization
        key_statistics = ['mean', 'median', 'p05', 'p95']
        
        for stat_name in key_statistics:
            if stat_name in statistics:
                output_file = save_statistic_netcdf(
                    lat_out, lon_out, statistics[stat_name], 
                    stat_name, metadata, run_id, output_dir
                )
                if output_file:
                    saved_files.append(output_file)
        
        # Final report
        if saved_files:
            logger.info("\n" + "=" * 60)
            logger.info("🎉 SUCCESS! Ensemble statistics created for QGIS")
            logger.info(f"📅 Forecast run: {run_id}")
            logger.info(f"⏰ Forecast step: {FORECAST_STEP} (+14 hours)")
            logger.info(f"🎲 Ensemble members: {len(ensemble_data)}")
            logger.info(f"📏 Grid: {len(lat_out)} × {len(lon_out)} points")
            logger.info(f"🗺️  Domain: {OUTPUT_BOUNDS['lat_min']}-{OUTPUT_BOUNDS['lat_max']}°N, {OUTPUT_BOUNDS['lon_min']}-{OUTPUT_BOUNDS['lon_max']}°E")
            logger.info(f"🔍 Resolution: {OUTPUT_RESOLUTION}° (~{OUTPUT_RESOLUTION * 111:.1f} km)")
            logger.info("\n📁 Created files:")
            for file_path in saved_files:
                file_size = Path(file_path).stat().st_size / 1024 / 1024
                logger.info(f"   • {Path(file_path).name} ({file_size:.1f} MB)")
            
            logger.info("\n📋 To use in QGIS:")
            logger.info("1. Open QGIS")
            logger.info("2. For each file: Layer → Add Raster Layer")
            logger.info("3. Select file and choose 'precipitation' variable")
            logger.info("4. CRS should auto-detect as EPSG:4326")
            logger.info("5. Style each layer with different colors:")
            logger.info("   • Mean: Blue color ramp")
            logger.info("   • Median: Green color ramp")
            logger.info("   • P05 (dry scenario): Light colors")
            logger.info("   • P95 (wet scenario): Dark/intense colors")
            
            return 0
        else:
            logger.error("❌ No files were saved successfully")
            return 1
            
    finally:
        # Cleanup temporary files
        try:
            import shutil
            shutil.rmtree(temp_dir)
            logger.info(f"🗑️  Cleaned up temporary directory")
        except:
            pass

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)