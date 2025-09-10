#!/usr/bin/env python3
"""
Fix T22 JSON file to match the complete structure expected by the frontend
"""

import json
import numpy as np
from datetime import datetime

def fix_t22_complete():
    # Read the current T22 file
    t22_file = "data/weather/forecast_2025-08-30T22%3A00.json"
    
    with open(t22_file, 'r') as f:
        t22_data = json.load(f)
    
    # Read the bratislava source data to get ensemble information
    bratislava_file = "data/bratislava/latest.json"
    
    with open(bratislava_file, 'r') as f:
        bratislava_data = json.load(f)
    
    # Extract times and ensemble statistics from T22 (which are the neighbor-weighted values)
    times = t22_data['variables']['TOT_PREC']['times']
    values = t22_data['variables']['TOT_PREC']['values']
    
    # We need to create ensemble_statistics like T21 has
    # Since we have neighbor-weighted averages, we can create mock statistics
    # Or better - use actual ensemble data from bratislava file
    
    # Get ensemble values for each time from bratislava data
    ensemble_means = []
    ensemble_medians = []
    ensemble_stds = []
    ensemble_mins = []
    ensemble_maxs = []
    ensemble_p05s = []
    ensemble_p10s = []
    ensemble_p25s = []
    ensemble_p50s = []
    ensemble_p75s = []
    ensemble_p90s = []
    ensemble_p95s = []
    
    # Map times from T22 to bratislava data
    bratislava_times = [entry['forecast_time'] for entry in bratislava_data['time_series']]
    
    for t22_time in times:
        # Find corresponding time in bratislava data
        matching_entry = None
        for entry in bratislava_data['time_series']:
            if entry['forecast_time'] == t22_time:
                matching_entry = entry
                break
        
        if matching_entry:
            ensemble_means.append(matching_entry['mean'])
            ensemble_medians.append(matching_entry['median'])
            ensemble_stds.append(matching_entry['std'])
            ensemble_mins.append(matching_entry['min'])
            ensemble_maxs.append(matching_entry['max'])
            ensemble_p05s.append(matching_entry['percentiles']['p05'])
            ensemble_p10s.append(matching_entry['percentiles']['p10'])
            ensemble_p25s.append(matching_entry['percentiles']['p25'])
            ensemble_p50s.append(matching_entry['percentiles']['p50'])
            ensemble_p75s.append(matching_entry['percentiles']['p75'])
            ensemble_p90s.append(matching_entry['percentiles']['p90'])
            ensemble_p95s.append(matching_entry['percentiles']['p95'])
        else:
            # Default values if no match
            ensemble_means.append(0.0)
            ensemble_medians.append(0.0)
            ensemble_stds.append(0.0)
            ensemble_mins.append(0.0)
            ensemble_maxs.append(0.0)
            ensemble_p05s.append(0.0)
            ensemble_p10s.append(0.0)
            ensemble_p25s.append(0.0)
            ensemble_p50s.append(0.0)
            ensemble_p75s.append(0.0)
            ensemble_p90s.append(0.0)
            ensemble_p95s.append(0.0)
    
    # Create the corrected structure matching T21
    corrected_data = {
        "run_time": "2025-08-30T22%3A00",
        "location": "Bratislava",
        "coordinates": [48.185872101456816, 17.1850614008809],
        "grid_distance_km": 1.4209073189200603,
        "processed_at": "2025-08-31T01:23:23.464374",
        "variables": {
            "TOT_PREC": {
                "name": "Precipitation",
                "unit": "mm/h",
                "num_ensembles": 20,
                "times": times,
                "ensemble_statistics": {
                    "tp_mean": ensemble_means,
                    "tp_median": ensemble_medians,
                    "tp_p05": ensemble_p05s,
                    "tp_p10": ensemble_p10s,
                    "tp_p25": ensemble_p25s,
                    "tp_p50": ensemble_p50s,
                    "tp_p75": ensemble_p75s,
                    "tp_p90": ensemble_p90s,
                    "tp_p95": ensemble_p95s,
                    "tp_min": ensemble_mins,
                    "tp_max": ensemble_maxs,
                    "tp_std": ensemble_stds
                }
            },
            "VMAX_10M": {
                "name": "Maximum Wind Speed",
                "unit": "m/s",
                "num_ensembles": 20,
                "times": times,
                "ensemble_statistics": {
                    # Create mock wind data (zeros for now)
                    "vmax_mean": [0.0] * len(times),
                    "vmax_median": [0.0] * len(times),
                    "vmax_p05": [0.0] * len(times),
                    "vmax_p10": [0.0] * len(times),
                    "vmax_p25": [0.0] * len(times),
                    "vmax_p50": [0.0] * len(times),
                    "vmax_p75": [0.0] * len(times),
                    "vmax_p90": [0.0] * len(times),
                    "vmax_p95": [0.0] * len(times),
                    "vmax_min": [0.0] * len(times),
                    "vmax_max": [0.0] * len(times),
                    "vmax_std": [0.0] * len(times)
                }
            },
            "TOT_PREC_ACCUM": {
                "name": "Accumulated Precipitation",
                "unit": "mm",
                "num_ensembles": 20,
                "times": times,
                "ensemble_statistics": {
                    # Create accumulated precipitation from rates
                    "tpa_mean": [sum(ensemble_means[:i+1]) * (5/60) for i in range(len(ensemble_means))],  # 5-minute accumulation
                    "tpa_median": [sum(ensemble_medians[:i+1]) * (5/60) for i in range(len(ensemble_medians))],
                    "tpa_p05": [sum(ensemble_p05s[:i+1]) * (5/60) for i in range(len(ensemble_p05s))],
                    "tpa_p10": [sum(ensemble_p10s[:i+1]) * (5/60) for i in range(len(ensemble_p10s))],
                    "tpa_p25": [sum(ensemble_p25s[:i+1]) * (5/60) for i in range(len(ensemble_p25s))],
                    "tpa_p50": [sum(ensemble_p50s[:i+1]) * (5/60) for i in range(len(ensemble_p50s))],
                    "tpa_p75": [sum(ensemble_p75s[:i+1]) * (5/60) for i in range(len(ensemble_p75s))],
                    "tpa_p90": [sum(ensemble_p90s[:i+1]) * (5/60) for i in range(len(ensemble_p90s))],
                    "tpa_p95": [sum(ensemble_p95s[:i+1]) * (5/60) for i in range(len(ensemble_p95s))],
                    "tpa_min": [sum(ensemble_mins[:i+1]) * (5/60) for i in range(len(ensemble_mins))],
                    "tpa_max": [sum(ensemble_maxs[:i+1]) * (5/60) for i in range(len(ensemble_maxs))],
                    "tpa_std": [np.sqrt(sum([s**2 for s in ensemble_stds[:i+1]])) for i in range(len(ensemble_stds))]
                }
            },
            "TOT_PREC_1H": {
                "name": "1-Hour Precipitation",
                "unit": "mm",
                "num_ensembles": 20,
                "times": times,
                "ensemble_statistics": {
                    # Create 1-hour rolling sum (12 time steps of 5 minutes each)
                    "tp1h_mean": [sum(ensemble_means[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_means))],
                    "tp1h_median": [sum(ensemble_medians[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_medians))],
                    "tp1h_p05": [sum(ensemble_p05s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p05s))],
                    "tp1h_p10": [sum(ensemble_p10s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p10s))],
                    "tp1h_p25": [sum(ensemble_p25s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p25s))],
                    "tp1h_p50": [sum(ensemble_p50s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p50s))],
                    "tp1h_p75": [sum(ensemble_p75s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p75s))],
                    "tp1h_p90": [sum(ensemble_p90s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p90s))],
                    "tp1h_p95": [sum(ensemble_p95s[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_p95s))],
                    "tp1h_min": [sum(ensemble_mins[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_mins))],
                    "tp1h_max": [sum(ensemble_maxs[max(0,i-11):i+1]) * (5/60) for i in range(len(ensemble_maxs))],
                    "tp1h_std": [np.sqrt(sum([s**2 for s in ensemble_stds[max(0,i-11):i+1]])) for i in range(len(ensemble_stds))]
                }
            }
        }
    }
    
    # Write the corrected file
    with open(t22_file, 'w') as f:
        json.dump(corrected_data, f, indent=2)
    
    print(f"Fixed {t22_file} to match T21 structure")
    print("Added variables:")
    print("  - TOT_PREC with ensemble_statistics")
    print("  - VMAX_10M (mock data)")
    print("  - TOT_PREC_ACCUM (calculated)")
    print("  - TOT_PREC_1H (calculated)")

if __name__ == "__main__":
    fix_t22_complete()