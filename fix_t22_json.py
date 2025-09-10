#!/usr/bin/env python3
"""
Convert T22 JSON file to match older weather schema
"""

import json
import os
from datetime import datetime

def fix_t22_json():
    input_file = "data/weather/forecast_2025-08-30T22%3A00.json"
    
    # Read the incorrect format
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Extract basic info
    location = data['metadata']['location']
    actual_coord_str = data['metadata']['actual_coordinates']  # "48.1513°N, 17.0984°E"
    lat_str, lon_str = actual_coord_str.split(', ')
    latitude = float(lat_str.replace('°N', '').replace('°S', ''))
    longitude = float(lon_str.replace('°E', '').replace('°W', ''))
    
    # Get grid distance
    grid_distance_km = float(data['metadata']['coordinate_accuracy'].replace(' km', ''))
    
    # Build times and values arrays
    times = []
    values = []
    
    for entry in data['time_series']:
        forecast_dt = datetime.fromisoformat(entry['forecast_time'].replace('Z', '+00:00'))
        times.append(forecast_dt.isoformat())
        values.append(entry['mean'])  # Use ensemble mean
    
    # Create the correct schema matching older files
    corrected_data = {
        "run_time": "2025-08-30T22%3A00",
        "location": location,
        "coordinates": [latitude, longitude],
        "grid_distance_km": grid_distance_km,
        "processed_at": data['metadata']['creation_date'],
        "variables": {
            "TOT_PREC": {
                "name": "Precipitation",
                "unit": "mm/h", 
                "num_ensembles": data['metadata']['ensemble_members'],
                "times": times,
                "values": values
            }
        }
    }
    
    # Write the corrected file
    with open(input_file, 'w') as f:
        json.dump(corrected_data, f, indent=2)
    
    print(f"Fixed {input_file} to match older weather schema")
    print(f"Total data points: {len(times)}")

if __name__ == "__main__":
    fix_t22_json()