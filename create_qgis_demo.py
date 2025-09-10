#!/usr/bin/env python3
"""
Create QGIS Demo with Synthetic Data
===================================

Creates a demonstration NetCDF file with synthetic precipitation data on the 
ICON-D2 grid domain for QGIS visualization. This is useful when live GRIB 
files are not available.

The synthetic data shows realistic precipitation patterns across Central Europe.
"""

import os
import sys
import numpy as np
import xarray as xr
from pathlib import Path
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Grid configuration
GRID_FILE = "data/grid/icon_grid_0047_R19B07_L.nc"

# Output grid configuration  
OUTPUT_RESOLUTION = 0.02  # degrees (about 2.2 km)
OUTPUT_BOUNDS = {
    'lat_min': 47.0,
    'lat_max': 55.0, 
    'lon_min': 5.0,
    'lon_max': 20.0
}

def load_icon_grid():
    """Load ICON grid coordinates"""
    logger.info("📐 Loading ICON grid coordinates...")
    
    grid_path = Path(GRID_FILE)
    if not grid_path.exists():
        logger.error(f"❌ Grid file not found: {GRID_FILE}")
        logger.info("Run the main script first to download the grid definition.")
        return None, None
    
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

def generate_synthetic_precipitation(lats, lons):
    """Generate realistic synthetic precipitation data"""
    logger.info("🌧️  Generating synthetic precipitation patterns...")
    
    # Create realistic precipitation patterns
    precip = np.zeros_like(lats)
    
    # Pattern 1: Alpine precipitation (higher in mountains)
    alpine_mask = (lats > 46) & (lats < 49) & (lons > 7) & (lons < 14)
    precip[alpine_mask] += 0.8 * np.random.exponential(0.3, np.sum(alpine_mask))
    
    # Pattern 2: Atlantic frontal system (west to east gradient)
    atlantic_effect = np.exp(-(lons - 5) / 4) * 0.5
    precip += atlantic_effect * np.random.exponential(0.4, len(lats))
    
    # Pattern 3: Convective cells (random hot spots)
    n_cells = 15
    for i in range(n_cells):
        # Random cell center
        cell_lat = np.random.uniform(47.5, 54)
        cell_lon = np.random.uniform(6, 19)
        cell_intensity = np.random.exponential(1.5)
        
        # Distance from cell center
        distances = np.sqrt((lats - cell_lat)**2 + (lons - cell_lon)**2)
        cell_influence = np.exp(-distances / 0.3) * cell_intensity
        precip += cell_influence
    
    # Pattern 4: North Sea effect
    north_sea_mask = (lats > 53) & (lons > 7) & (lons < 12)
    precip[north_sea_mask] += 0.3 * np.random.exponential(0.5, np.sum(north_sea_mask))
    
    # Add some random noise
    precip += np.random.exponential(0.1, len(lats))
    
    # Smooth the data a bit (simple spatial filter)
    for _ in range(2):  # Apply twice for smoother result
        precip_smooth = precip.copy()
        for i in range(len(lats)):
            # Find nearby points (crude spatial smoothing)
            distances = np.sqrt((lats - lats[i])**2 + (lons - lons[i])**2)
            nearby_mask = distances < 0.1  # Within ~11km
            if np.sum(nearby_mask) > 1:
                precip_smooth[i] = np.mean(precip[nearby_mask])
        precip = precip_smooth
    
    # Ensure non-negative and reasonable values
    precip = np.maximum(precip, 0)
    precip = np.minimum(precip, 12)  # Cap at 12 mm/h
    
    logger.info(f"✅ Generated precipitation field")
    logger.info(f"   Value range: {precip.min():.3f} to {precip.max():.3f} mm/h")
    logger.info(f"   Mean: {precip.mean():.3f} mm/h")
    logger.info(f"   Non-zero points: {np.sum(precip > 0.01)} / {len(precip)} ({100*np.sum(precip > 0.01)/len(precip):.1f}%)")
    
    return precip

def regrid_to_latlon(lats, lons, data):
    """Regrid synthetic data to regular lat/lon grid"""
    logger.info("🌐 Regridding to regular lat/lon grid...")
    
    try:
        from scipy.interpolate import griddata
        
        # Create output grid
        lat_out = np.arange(OUTPUT_BOUNDS['lat_min'], OUTPUT_BOUNDS['lat_max'], OUTPUT_RESOLUTION)
        lon_out = np.arange(OUTPUT_BOUNDS['lon_min'], OUTPUT_BOUNDS['lon_max'], OUTPUT_RESOLUTION)
        lon_grid, lat_grid = np.meshgrid(lon_out, lat_out)
        
        logger.info(f"📏 Output grid: {len(lat_out)} × {len(lon_out)} = {len(lat_out) * len(lon_out)} points")
        
        # Filter source points to output bounds + margin
        mask = ((lats >= OUTPUT_BOUNDS['lat_min'] - 0.5) & 
                (lats <= OUTPUT_BOUNDS['lat_max'] + 0.5) &
                (lons >= OUTPUT_BOUNDS['lon_min'] - 0.5) & 
                (lons <= OUTPUT_BOUNDS['lon_max'] + 0.5) &
                np.isfinite(data))
        
        lats_filtered = lats[mask]
        lons_filtered = lons[mask]  
        data_filtered = data[mask]
        
        logger.info(f"🎯 Using {len(data_filtered)} filtered points for interpolation")
        
        if len(data_filtered) < 10:
            raise Exception("Too few valid data points for interpolation")
        
        # Interpolate
        logger.info("🔄 Interpolating data...")
        
        points = np.column_stack((lats_filtered, lons_filtered))
        grid_points = np.column_stack((lat_grid.ravel(), lon_grid.ravel()))
        
        # Use linear interpolation 
        interpolated = griddata(
            points, data_filtered, grid_points,
            method='linear', fill_value=0
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
        logger.info(f"   Value range: {np.nanmin(interpolated):.3f} to {np.nanmax(interpolated):.3f}")
        
        return lat_out, lon_out, interpolated
        
    except ImportError:
        logger.error("❌ scipy not available. Install with: pip install scipy")
        return None, None, None
    except Exception as e:
        logger.error(f"❌ Error regridding: {e}")
        return None, None, None

def save_as_netcdf(lat, lon, data, output_file):
    """Save regridded data as NetCDF with proper CRS for QGIS"""
    logger.info(f"💾 Saving NetCDF file: {output_file}")
    
    try:
        # Create xarray dataset
        ds = xr.Dataset(
            {
                'precipitation': (['lat', 'lon'], data, {
                    'long_name': 'Synthetic Precipitation Rate',
                    'units': 'mm/h',
                    'grid_mapping': 'crs',
                    'description': 'Synthetic precipitation data for QGIS demonstration'
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
                'time': np.datetime64('now')
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
            'title': 'ICON-D2 Domain Synthetic Precipitation Demo',
            'description': 'Synthetic precipitation data on ICON-D2 grid domain for QGIS visualization',
            'source': 'Synthetic data generated for demonstration purposes',
            'institution': 'ICON-RUC Processing Pipeline',
            'conventions': 'CF-1.8',
            'created': datetime.now().isoformat(),
            'grid_resolution_degrees': OUTPUT_RESOLUTION,
            'original_grid': 'ICON unstructured triangular grid (synthetic data)',
            'interpolation_method': 'linear with nearest neighbor fallback',
            'crs': 'EPSG:4326',
            'domain': 'Central Europe (Germany and surroundings)',
            'data_type': 'synthetic_demonstration'
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
    logger.info("🌧️  Creating QGIS Demo with Synthetic Data")
    logger.info("=" * 60)
    
    # Load ICON grid
    lats, lons = load_icon_grid()
    if lats is None:
        return 1
    
    # Generate synthetic precipitation
    precip_data = generate_synthetic_precipitation(lats, lons)
    
    # Regrid to lat/lon
    lat_out, lon_out, data_out = regrid_to_latlon(lats, lons, precip_data)
    if data_out is None:
        return 1
    
    # Save as NetCDF
    output_dir = Path("data/qgis_samples")
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"icon_d2_synthetic_precipitation_demo_{timestamp}.nc"
    
    success = save_as_netcdf(lat_out, lon_out, data_out, str(output_file))
    
    if success:
        logger.info("\n" + "=" * 60)
        logger.info("🎉 SUCCESS! Synthetic NetCDF demo created for QGIS")
        logger.info(f"📁 File: {output_file}")
        logger.info(f"📏 Grid: {len(lat_out)} × {len(lon_out)} points")
        logger.info(f"🗺️  Bounds: {OUTPUT_BOUNDS['lat_min']}-{OUTPUT_BOUNDS['lat_max']}°N, {OUTPUT_BOUNDS['lon_min']}-{OUTPUT_BOUNDS['lon_max']}°E")
        logger.info(f"🔍 Resolution: {OUTPUT_RESOLUTION}° (~{OUTPUT_RESOLUTION * 111:.1f} km)")
        logger.info(f"🌧️  Data: Synthetic precipitation with realistic patterns")
        logger.info("\n📋 To use in QGIS:")
        logger.info("1. Open QGIS")
        logger.info("2. Layer → Add Layer → Add Raster Layer")
        logger.info(f"3. Select: {output_file}")
        logger.info("4. Choose 'precipitation' variable")
        logger.info("5. CRS should be automatically detected as EPSG:4326")
        logger.info("6. Style → Singleband pseudocolor → Apply")
        logger.info("\n🎨 Suggested QGIS styling:")
        logger.info("- Min: 0, Max: 5 mm/h for typical precipitation")
        logger.info("- Color ramp: Blues to Reds")
        logger.info("- Classification: Equal intervals")
        
        return 0
    else:
        return 1

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)