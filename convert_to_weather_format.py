#!/usr/bin/env python3
"""
Convert bratislava format JSON to weather directory format
"""

import json
import os
from datetime import datetime

def convert_bratislava_to_weather_format(input_file, output_dir="data/weather"):
    """
    Convert bratislava latest.json format to weather directory format with ensemble files
    """
    # Read the bratislava format data
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Extract metadata and run_time from first entry
    first_entry = data['time_series'][0]
    run_time = first_entry['run_time']
    location_name = data['metadata']['location']
    
    # Parse coordinates from metadata - use actual coordinates from the data if available
    if 'actual_coordinates' in data['metadata']:
        coord_str = data['metadata']['actual_coordinates']  # "48.1513°N, 17.0984°E"
    else:
        coord_str = data['metadata']['target_coordinates']  # "48.1486°N, 17.1077°E"
    
    lat_str, lon_str = coord_str.split(', ')
    latitude = float(lat_str.replace('°N', '').replace('°S', ''))
    longitude = float(lon_str.replace('°E', '').replace('°W', ''))
    
    # Parse run_time to datetime and create directory name with URL encoding
    run_dt = datetime.fromisoformat(run_time.replace('Z', '+00:00'))
    dir_name = f"forecast_{run_dt.strftime('%Y-%m-%dT%H')}%3A00"
    output_path = os.path.join(output_dir, dir_name)
    
    # Ensure output directory exists
    os.makedirs(output_path, exist_ok=True)
    
    # Get coordinate accuracy/distance from metadata
    accuracy_str = data['metadata'].get('coordinate_accuracy', '0 km')
    grid_distance_km = float(accuracy_str.replace(' km', ''))
    
    # Prepare times array
    times = []
    all_ensemble_values = []
    
    # Convert time_series data
    for entry in data['time_series']:
        # Parse forecast_time
        forecast_dt = datetime.fromisoformat(entry['forecast_time'].replace('Z', '+00:00'))
        times.append(forecast_dt.isoformat())
        
        # Get ensemble values for this time step (flatten first level)
        ensemble_values = entry.get('ensemble_values', [[]])
        if ensemble_values and len(ensemble_values) > 0:
            all_ensemble_values.append(ensemble_values[0])  # Take first (and only) nested array
        else:
            all_ensemble_values.append([])
    
    # Create individual ensemble files (01-20)
    num_ensembles = len(all_ensemble_values[0]) if all_ensemble_values else 20
    
    for ensemble_idx in range(num_ensembles):
        ensemble_id = f"{ensemble_idx + 1:02d}"
        
        # Extract values for this ensemble across all times
        ensemble_time_series = []
        for time_idx, time_values in enumerate(all_ensemble_values):
            if time_values and ensemble_idx < len(time_values):
                ensemble_time_series.append(time_values[ensemble_idx])
            else:
                ensemble_time_series.append(0.0)
        
        # Create ensemble file structure
        ensemble_data = {
            "run_time": f"{run_dt.strftime('%Y-%m-%dT%H')}%3A00",
            "location": location_name,
            "coordinates": [latitude, longitude],
            "grid_distance_km": grid_distance_km,
            "ensemble_id": ensemble_id,
            "variable": "TOT_PREC",
            "name": "Precipitation",
            "unit": "mm/h",
            "times": times,
            "values": ensemble_time_series
        }
        
        # Write ensemble file
        ensemble_filename = f"TOT_PREC_ensemble_{ensemble_id}.json"
        ensemble_filepath = os.path.join(output_path, ensemble_filename)
        
        with open(ensemble_filepath, 'w') as f:
            json.dump(ensemble_data, f, indent=2)
    
    print(f"Converted {input_file} to {output_path}")
    print(f"Created {num_ensembles} ensemble files with {len(times)} time steps each")
    
    return output_path

if __name__ == "__main__":
    input_file = "data/bratislava/latest.json"
    if os.path.exists(input_file):
        convert_bratislava_to_weather_format(input_file)
    else:
        print(f"Input file {input_file} not found")