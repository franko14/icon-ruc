#!/usr/bin/env python3
"""
Create QGIS-Compatible NetCDF Sample
===================================

Downloads one sample GRIB file from ICON-D2-RUC-EPS, regrids it to a regular 
lat/lon grid, and saves it as NetCDF with proper CRS for QGIS visualization.

This creates a full spatial grid (not just a single point) so you can visualize
the precipitation field across the entire ICON-D2 domain in QGIS.
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
    from scipy.spatial import cKDTree
    from scipy.interpolate import griddata
    SCIPY_AVAILABLE = True
except ImportError:
    logger.error("scipy not available. Install with: pip install scipy")
    SCIPY_AVAILABLE = False
    sys.exit(1)

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

def get_sample_grib_url():
    """Get a direct URL to a known GRIB file"""
    # Use the URL provided by the user - a later forecast step that should be available
    sample_url = "https://opendata.dwd.de/weather/nwp/v1/m/icon-d2-ruc-eps/p/TOT_PREC/r/2025-08-30T16%3A00/e/16/s/PT014H00M.grib2"
    run_id = "2025-08-30T16%3A00"
    filename = "PT014H00M.grib2"
    
    logger.info(f"📁 Using direct URL: {sample_url}")
    logger.info(f"📅 Run: {run_id}")
    logger.info(f"⏰ Forecast step: PT014H00M (14 hours)")
    logger.info(f"🎲 Ensemble: 16")
    
    # Test if the URL is accessible
    try:
        response = requests.head(sample_url, timeout=10)
        if response.status_code == 200:
            file_size = response.headers.get('content-length', 'unknown')
            logger.info(f"✅ File accessible, size: {file_size} bytes")
            return run_id, sample_url, filename
        else:
            logger.warning(f"⚠️ File may not be available (status: {response.status_code})")
            return run_id, sample_url, filename
    except Exception as e:
        logger.warning(f"⚠️ Could not verify URL accessibility: {e}")
        return run_id, sample_url, filename

def download_sample_grib(url, filename):
    """Download the sample GRIB file"""
    logger.info(f"📥 Downloading sample GRIB file...")
    
    try:
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        
        temp_path = Path(tempfile.mkdtemp()) / filename
        with open(temp_path, 'wb') as f:
            f.write(response.content)
        
        logger.info(f"✅ Downloaded: {temp_path} ({temp_path.stat().st_size / 1024 / 1024:.1f} MB)")
        return str(temp_path)
        
    except Exception as e:
        logger.error(f"❌ Error downloading GRIB file: {e}")
        return None

def load_icon_grid(grid_file):
    """Load ICON grid coordinates"""
    logger.info("📐 Loading ICON grid coordinates...")
    
    try:
        grid_ds = xr.open_dataset(grid_file)
        
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

def regrid_to_latlon(grib_file, lats, lons):
    """Regrid GRIB data to regular lat/lon grid"""
    logger.info("🌐 Regridding to regular lat/lon grid...")
    
    try:
        # Open GRIB file
        ds = xr.open_dataset(grib_file, engine='cfgrib')
        
        # Find precipitation variable
        var_names = list(ds.data_vars.keys())
        logger.info(f"Available variables: {var_names}")
        
        # Try to find precipitation variable
        prec_var = None
        for var in ['tp', 'unknown', 'precipitation', 'tot_prec']:
            if var in var_names:
                prec_var = var
                break
        
        if prec_var is None and var_names:
            prec_var = var_names[0]
            logger.warning(f"Using first available variable: {prec_var}")
        
        if prec_var is None:
            raise Exception("No variables found in GRIB file")
        
        # Get the data
        data = ds[prec_var].values
        
        if data.ndim == 1:
            # Unstructured grid data
            logger.info(f"📊 Processing {prec_var}: {len(data)} grid points")
        else:
            # Flatten if needed
            data = data.flatten()
            logger.info(f"📊 Processing {prec_var}: {data.shape} → {len(data)} points")
        
        # Create output grid
        lat_out = np.arange(OUTPUT_BOUNDS['lat_min'], OUTPUT_BOUNDS['lat_max'], OUTPUT_RESOLUTION)
        lon_out = np.arange(OUTPUT_BOUNDS['lon_min'], OUTPUT_BOUNDS['lon_max'], OUTPUT_RESOLUTION)
        lon_grid, lat_grid = np.meshgrid(lon_out, lat_out)
        
        logger.info(f"📏 Output grid: {len(lat_out)} × {len(lon_out)} = {len(lat_out) * len(lon_out)} points")
        
        # Filter source points to reasonable bounds
        mask = ((lats >= OUTPUT_BOUNDS['lat_min'] - 1) & 
                (lats <= OUTPUT_BOUNDS['lat_max'] + 1) &
                (lons >= OUTPUT_BOUNDS['lon_min'] - 1) & 
                (lons <= OUTPUT_BOUNDS['lon_max'] + 1) &
                np.isfinite(data))
        
        lats_filtered = lats[mask]
        lons_filtered = lons[mask]
        data_filtered = data[mask]
        
        logger.info(f"🎯 Using {len(data_filtered)} filtered points for interpolation")
        
        if len(data_filtered) < 10:
            raise Exception("Too few valid data points for interpolation")
        
        # Interpolate using scipy.interpolate.griddata
        logger.info("🔄 Interpolating data (this may take a few minutes)...")
        
        points = np.column_stack((lats_filtered, lons_filtered))
        grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
        
        # Use linear interpolation with nearest neighbor fallback
        interpolated = griddata(
            points, data_filtered, grid_points, 
            method='linear', fill_value=np.nan
        )
        
        # Fill NaN values with nearest neighbor
        nan_mask = np.isnan(interpolated)
        if np.any(nan_mask):
            logger.info("🔧 Filling gaps with nearest neighbor...")
            nearest = griddata(
                points, data_filtered, grid_points, 
                method='nearest', fill_value=0
            )
            interpolated[nan_mask] = nearest[nan_mask]
        
        # Reshape to grid
        interpolated = interpolated.reshape(lat_grid.shape)
        
        logger.info(f"✅ Regridding complete")
        logger.info(f"   Output shape: {interpolated.shape}")
        logger.info(f"   Value range: {np.nanmin(interpolated):.6f} to {np.nanmax(interpolated):.6f}")
        
        # Get metadata
        metadata = {
            'variable_name': prec_var,
            'long_name': ds[prec_var].attrs.get('long_name', 'Total Precipitation'),
            'units': ds[prec_var].attrs.get('units', 'kg m**-2'),
            'time': ds.time.values if 'time' in ds.coords else np.datetime64('now'),
            'step': ds.step.values if 'step' in ds.coords else np.timedelta64(0, 'h')
        }
        
        ds.close()
        return lat_out, lon_out, interpolated, metadata
        
    except Exception as e:
        logger.error(f"❌ Error regridding: {e}")
        return None, None, None, None

def save_as_netcdf(lat, lon, data, metadata, output_file):
    """Save regridded data as NetCDF with proper CRS for QGIS"""
    logger.info(f"💾 Saving NetCDF file: {output_file}")
    
    try:
        # Create xarray dataset
        ds = xr.Dataset(
            {
                'precipitation': (['lat', 'lon'], data, {
                    'long_name': metadata['long_name'],
                    'units': metadata['units'],
                    'grid_mapping': 'crs'
                })
            },
            coords={
                'lat': (['lat'], lat, {
                    'long_name': 'Latitude',
                    'units': 'degrees_north',
                    'axis': 'Y'
                }),
                'lon': (['lon'], lon, {
                    'long_name': 'Longitude', 
                    'units': 'degrees_east',
                    'axis': 'X'
                }),
                'time': metadata['time']
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
            'title': 'ICON-D2-RUC-EPS Precipitation Sample',
            'description': 'Sample precipitation data regridded from ICON unstructured grid to regular lat/lon',
            'source': 'DWD ICON-D2-RUC-EPS',
            'institution': 'Deutscher Wetterdienst (DWD)',
            'conventions': 'CF-1.8',
            'created': datetime.now().isoformat(),
            'grid_resolution_degrees': OUTPUT_RESOLUTION,
            'original_grid': 'ICON unstructured triangular grid',
            'interpolation_method': 'linear with nearest neighbor fallback',
            'crs': 'EPSG:4326'
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
        
        file_size = Path(output_file).stat().st_size / 1024 / 1024
        logger.info(f"✅ NetCDF saved: {file_size:.1f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error saving NetCDF: {e}")
        return False

def main():
    """Main function"""
    logger.info("🌧️  Creating QGIS-compatible precipitation sample")
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
    
    # Get sample GRIB file URL
    run_id, sample_url, filename = get_sample_grib_url()
    
    # Download sample GRIB file
    grib_file = download_sample_grib(sample_url, filename)
    if not grib_file:
        return 1
    
    try:
        # Load ICON grid
        lats, lons = load_icon_grid(grid_file)
        if lats is None:
            return 1
        
        # Regrid to lat/lon
        lat_out, lon_out, data_out, metadata = regrid_to_latlon(grib_file, lats, lons)
        if data_out is None:
            return 1
        
        # Save as NetCDF
        output_dir = Path("data/qgis_samples")
        output_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = output_dir / f"icon_d2_precipitation_sample_{run_id.replace('%3A', '_').replace('T', '_')}.nc"
        
        success = save_as_netcdf(lat_out, lon_out, data_out, metadata, str(output_file))
        
        if success:
            logger.info("\n" + "=" * 60)
            logger.info("🎉 SUCCESS! NetCDF file created for QGIS")
            logger.info(f"📁 File: {output_file}")
            logger.info(f"📏 Grid: {len(lat_out)} × {len(lon_out)} points")
            logger.info(f"🗺️  Bounds: {OUTPUT_BOUNDS['lat_min']}-{OUTPUT_BOUNDS['lat_max']}°N, {OUTPUT_BOUNDS['lon_min']}-{OUTPUT_BOUNDS['lon_max']}°E")
            logger.info(f"🔍 Resolution: {OUTPUT_RESOLUTION}° (~{OUTPUT_RESOLUTION * 111:.1f} km)")
            logger.info("\n📋 To use in QGIS:")
            logger.info("1. Open QGIS")
            logger.info("2. Layer → Add Layer → Add Raster Layer")
            logger.info(f"3. Select: {output_file}")
            logger.info("4. Choose 'precipitation' variable")
            logger.info("5. CRS should be automatically detected as EPSG:4326")
            
            return 0
        else:
            return 1
            
    finally:
        # Cleanup temporary files
        try:
            if os.path.exists(grib_file):
                os.unlink(grib_file)
                logger.info(f"🗑️  Cleaned up temporary file: {grib_file}")
        except:
            pass

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)