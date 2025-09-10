#!/usr/bin/env python3
"""
NetCDF to JSON Converter for Web Dashboard
==========================================

Converts NetCDF output from bratislava_pipeline.py to JSON format 
that can be easily consumed by the web dashboard.
"""

import json
import numpy as np
import xarray as xr
from datetime import datetime, timezone
from pathlib import Path
import argparse
import sys

# Import validation models
try:
    from weather_models import WeatherForecastValidator, WeatherForecast
    VALIDATION_AVAILABLE = True
except ImportError:
    VALIDATION_AVAILABLE = False
    print("⚠️ Weather validation models not available - skipping validation")

def numpy_converter(obj):
    """Convert numpy types to native Python types for JSON serialization"""
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, np.datetime64):
        return str(obj)
    raise TypeError(f"Object of type {type(obj)} is not JSON serializable")

def convert_netcdf_to_json(netcdf_path, output_path=None):
    """
    Convert NetCDF file from pipeline to JSON for web dashboard.
    
    Args:
        netcdf_path (str): Path to NetCDF file
        output_path (str): Output JSON file path (optional)
    
    Returns:
        dict: JSON data structure
    """
    try:
        # Load NetCDF file
        print(f"📂 Loading NetCDF file: {netcdf_path}")
        ds = xr.open_dataset(netcdf_path)
        
        # Extract the main run time from dataset coordinates first, then attributes, then filename
        run_time_str = 'unknown'
        
        # Try to get from run_time coordinate
        if 'run_time' in ds.coords:
            run_time_coord = ds.coords['run_time']
            try:
                # Handle both scalar and array coordinates
                if run_time_coord.size > 0:
                    import pandas as pd
                    # Handle scalar coordinates (single value)
                    if run_time_coord.ndim == 0:
                        rt = pd.to_datetime(str(run_time_coord.values))
                    else:
                        rt = pd.to_datetime(str(run_time_coord.values[0]))
                    run_time_str = rt.strftime('%Y-%m-%dT%H%%3A%M')
            except Exception as e:
                print(f"   ⚠️ Could not parse run_time coordinate: {e}")
        
        # Try to get from attributes if coordinate method failed
        if run_time_str == 'unknown':
            run_time_str = ds.attrs.get('run_time', 'unknown')
        
        # Try to extract from filename as fallback
        if run_time_str == 'unknown':
            filename = Path(netcdf_path).name
            if '_' in filename:
                parts = filename.split('_')
                for part in parts:
                    if len(part) >= 8 and part[:8].isdigit():
                        # Found date part, format it
                        date_part = part[:8]
                        time_part = part[9:15] if len(part) > 9 else '000000'
                        run_time_str = f"{date_part[:4]}-{date_part[4:6]}-{date_part[6:8]}T{time_part[:2]}%3A{time_part[2:4]}"
                        break
        
        # Parse coordinates from attributes
        actual_coords_str = ds.attrs.get('actual_coordinates', 'N/A')
        coordinates = [48.1486, 17.1077]  # Default Bratislava coords
        if '°N' in actual_coords_str and '°E' in actual_coords_str:
            # Parse "48.1513°N, 17.0984°E" format
            parts = actual_coords_str.replace('°N', '').replace('°E', '').split(', ')
            if len(parts) == 2:
                try:
                    coordinates = [float(parts[0]), float(parts[1])]
                except ValueError:
                    pass
        
        # Parse grid distance
        grid_distance = 0.75  # Default
        coord_accuracy = ds.attrs.get('coordinate_accuracy', '0.75 km')
        if 'km' in coord_accuracy:
            try:
                grid_distance = float(coord_accuracy.replace(' km', ''))
            except ValueError:
                pass
        
        # Get time coordinates
        if 'run_time' in ds.sizes:
            run_times = ds.run_time.values
        else:
            run_times = [np.datetime64('now')]
        
        if 'step' in ds.sizes:
            steps = ds.step.values
        else:
            steps = [0]
        
        # Detect available weather variables in the dataset
        available_vars = {}
        precipitation_vars = ['tp', 'precipitation', 'total_precipitation', 'precip']
        wind_vars = ['vmax_10m', 'wind_speed_10m', 'wind_max_10m', 'max_wind_10m']
        
        for var_name in ds.data_vars:
            if any(pv in var_name.lower() for pv in precipitation_vars):
                # Check if we already found a precipitation variable, prefer the base 'tp' if available
                if 'TOT_PREC' not in available_vars or var_name == 'tp':
                    available_vars['TOT_PREC'] = {
                        'nc_var': 'tp' if var_name == 'tp' or var_name.startswith('tp_') else var_name,
                        'name': 'Precipitation', 
                        'unit': 'mm/h',
                        'needs_deaccumulation': True
                    }
            elif any(wv in var_name.lower() for wv in wind_vars):
                # Check if we already found a wind variable, prefer the base variable if available  
                if 'VMAX_10M' not in available_vars or 'vmax_10m' in var_name.lower():
                    base_wind_var = 'vmax_10m'
                    for wv in wind_vars:
                        if wv in var_name.lower():
                            base_wind_var = wv
                            break
                    available_vars['VMAX_10M'] = {
                        'nc_var': base_wind_var,
                        'name': 'Max Wind Speed at 10m',
                        'unit': 'm/s', 
                        'needs_deaccumulation': False
                    }
        
        print(f"🔍 Detected variables: {', '.join(available_vars.keys())}")
        
        if not available_vars:
            print("❌ No recognized weather variables found in dataset")
            ds.close()
            return None
        
        # Build time series for all forecast times (skip 0th step per user requirement)
        times = []
        for run_idx, run_time in enumerate(run_times):
            for step_idx, step in enumerate(steps):
                if step_idx == 0:  # Skip the 0th step (always 0 value)
                    continue
                # Calculate forecast time (step is in hours, convert to minutes for timedelta)
                forecast_minutes = int(step * 60)  # Convert hours to minutes
                forecast_time = np.datetime64(run_time) + np.timedelta64(forecast_minutes, 'm')
                forecast_time_str = str(forecast_time)[:19]  # Remove timezone info
                times.append(forecast_time_str)
        
        # Process each detected variable
        variables_data = {}
        
        # Get ensemble data and calculate statistics
        num_ensembles = ds.sizes.get('ensemble', 1)
        
        for var_id, var_info in available_vars.items():
            print(f"📊 Processing {var_id} ({var_info['name']})...")
            base_var = var_info['nc_var']  # e.g., 'tp' for precipitation
            
            # Find all statistical variables for this base variable in the dataset
            stat_vars = {}
            print(f"   🔍 Looking for variables starting with '{base_var}' and containing '_'...")
            for var_name in ds.data_vars:
                if var_name.startswith(base_var) and '_' in var_name:
                    # This is a statistical variable like tp_mean, tp_p05, etc.
                    stat_vars[var_name] = var_name
                    print(f"     ✅ Added {var_name}")
                else:
                    print(f"     ⏭️  Skipped {var_name} (startswith={var_name.startswith(base_var)}, has_underscore={'_' in var_name})")
            
            if not stat_vars:
                print(f"   ⚠️ No statistical variables found for {var_id}, trying raw processing...")
                # Fallback to raw variable if no statistics found
                stat_vars[f'{base_var}_mean'] = base_var
            
            print(f"   🔍 Found statistical variables: {list(stat_vars.keys())}")
            
            # Extract data from the pre-computed statistical variables
            ensemble_statistics = {}
            
            for stat_name, nc_var_name in stat_vars.items():
                values = []
                
                # Process each time step (skip 0th step per user requirement)
                for run_idx, run_time in enumerate(run_times):
                    for step_idx, step in enumerate(steps):
                        if step_idx == 0:  # Skip the 0th step (always 0 value)
                            continue
                            
                        # Extract statistical data for this time point
                        try:
                            if len(run_times) > 1:
                                value = float(ds[nc_var_name].isel(run_time=run_idx, step=step_idx).values)
                            else:
                                value = float(ds[nc_var_name].isel(step=step_idx).values)
                            values.append(value)
                        except Exception as e:
                            print(f"   ⚠️ Error extracting {nc_var_name} at step {step_idx}: {e}")
                            values.append(0.0)
                
                ensemble_statistics[stat_name] = values
            
            # Store variable data
            variables_data[var_id] = {
                'name': var_info['name'],
                'unit': var_info['unit'],
                'num_ensembles': num_ensembles,
                'times': times,
                'ensemble_statistics': ensemble_statistics
            }
        
        # Create the expected JSON structure
        json_data = {
            'run_time': run_time_str,
            'location': ds.attrs.get('location', 'Bratislava'),
            'coordinates': coordinates,
            'grid_distance_km': grid_distance,
            'processed_at': datetime.now(timezone.utc).isoformat(),
            'variables': variables_data
        }
        
        # Close dataset
        ds.close()
        
        # Validate JSON structure before saving
        if VALIDATION_AVAILABLE:
            try:
                validated_data = WeatherForecastValidator.validate_json_data(json_data)
                print("✅ JSON data validation passed")
            except Exception as e:
                print(f"❌ JSON validation failed: {e}")
                print("⚠️ Continuing with potentially invalid data...")
        
        # Save to file if output path provided
        if output_path:
            print(f"💾 Saving JSON to: {output_path}")
            with open(output_path, 'w') as f:
                json.dump(json_data, f, indent=2, default=numpy_converter)
            
            file_size = Path(output_path).stat().st_size / 1024
            print(f"   ✅ Saved {file_size:.1f} KB JSON file")
            
            # Validate saved file as final check
            if VALIDATION_AVAILABLE:
                try:
                    WeatherForecastValidator.validate_json_file(output_path)
                    print("✅ Saved file validation passed")
                except Exception as e:
                    print(f"❌ Saved file validation failed: {e}")
        
        print(f"✅ Conversion completed successfully")
        return json_data
        
    except Exception as e:
        print(f"❌ Error converting NetCDF to JSON: {e}")
        raise

def find_latest_netcdf(data_dir):
    """Find the most recent NetCDF file in the data directory"""
    data_path = Path(data_dir)
    if not data_path.exists():
        return None
    
    # Look for NetCDF files
    nc_files = list(data_path.glob("**/*.nc"))
    if not nc_files:
        return None
    
    # Return the most recent file
    latest_file = max(nc_files, key=lambda f: f.stat().st_mtime)
    return str(latest_file)

def main():
    parser = argparse.ArgumentParser(
        description='Convert NetCDF forecast data to JSON for web dashboard',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python netcdf_to_json.py data/bratislava/bratislava_precipitation_20250829_164523.nc
  python netcdf_to_json.py --latest
  python netcdf_to_json.py --input data.nc --output web_data.json
        """
    )
    
    parser.add_argument('input_file', nargs='?', 
                       help='Input NetCDF file path')
    parser.add_argument('--output', '-o', 
                       help='Output JSON file path (default: same name with .json extension)')
    parser.add_argument('--latest', action='store_true',
                       help='Find and convert the latest NetCDF file in data/bratislava/')
    parser.add_argument('--data-dir', default='data/bratislava',
                       help='Directory to search for latest file (default: data/bratislava)')
    parser.add_argument('--pretty', action='store_true',
                       help='Pretty-print JSON output')
    
    args = parser.parse_args()
    
    # Determine input file
    if args.latest:
        input_file = find_latest_netcdf(args.data_dir)
        if not input_file:
            print(f"❌ No NetCDF files found in {args.data_dir}")
            sys.exit(1)
        print(f"🔍 Found latest file: {Path(input_file).name}")
    elif args.input_file:
        input_file = args.input_file
    else:
        parser.print_help()
        sys.exit(1)
    
    # Check if input file exists
    if not Path(input_file).exists():
        print(f"❌ Input file not found: {input_file}")
        sys.exit(1)
    
    # Determine output file
    if args.output:
        output_file = args.output
    else:
        output_file = str(Path(input_file).with_suffix('.json'))
    
    try:
        # Convert file
        json_data = convert_netcdf_to_json(input_file, output_file)
        
        # Print summary
        print("\n📊 Conversion Summary:")
        print(f"   Location: {json_data['location']}")
        print(f"   Coordinates: {json_data['coordinates']}")
        print(f"   Ensemble members: {json_data['variables']['TOT_PREC']['num_ensembles']}")
        print(f"   Time steps: {len(json_data['variables']['TOT_PREC']['times'])}")
        print(f"   Output: {output_file}")
        
        # Also create a 'latest.json' symlink/copy for web dashboard
        latest_json = Path(args.data_dir) / 'latest.json'
        try:
            with open(latest_json, 'w') as f:
                json.dump(json_data, f, indent=2 if args.pretty else None, default=numpy_converter)
            print(f"   📂 Also saved as: {latest_json}")
        except Exception as e:
            print(f"   ⚠️ Could not create latest.json: {e}")
        
    except Exception as e:
        print(f"❌ Conversion failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()