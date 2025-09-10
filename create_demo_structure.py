#!/usr/bin/env python3
"""
Create demonstration of the expected weather data structure
"""

from pathlib import Path
from datetime import datetime, timedelta
import json
import numpy as np

from utils.models import (ForecastRun, VariableData, EnsembleMember, 
                         TargetLocation, get_variable_config, save_json)
from utils.statistics import calculate_ensemble_statistics
from utils.data_io import save_all_outputs
from utils.statistics import calculate_derived_precipitation_variables

def create_demo_forecast_run() -> ForecastRun:
    """Create a demonstration forecast run with realistic data"""
    
    # Create target location
    target = TargetLocation(
        lat=48.1486, 
        lon=17.1077, 
        name="Bratislava",
        grid_index=12345,
        distance_km=1.07
    )
    
    # Create times (skip 0th as requested)
    base_time = datetime(2025, 8, 31, 5, 0)
    times = []
    for i in range(1, 25):  # 24 time steps (skip 0th)
        time_step = base_time + timedelta(minutes=i*5)
        times.append(time_step.isoformat())
    
    # Create forecast run
    forecast_run = ForecastRun(
        run_time="2025-08-31T05:00",
        location=target,
        processed_at=datetime.utcnow().isoformat()
    )
    
    # Create precipitation data (TOT_PREC)
    prec_var = get_variable_config('TOT_PREC')
    prec_ensembles = []
    
    for ens_id in range(1, 21):  # 20 ensembles
        # Generate realistic precipitation data
        base_values = np.random.exponential(0.5, len(times))  # Exponential distribution for precip
        # Add some storm event in middle
        storm_peak = len(times) // 2
        for i in range(max(0, storm_peak-3), min(len(times), storm_peak+3)):
            base_values[i] += np.random.exponential(2.0)
        
        ensemble = EnsembleMember(
            ensemble_id=f"{ens_id:02d}",
            times=times,
            values=base_values.tolist()
        )
        prec_ensembles.append(ensemble)
    
    # Calculate statistics
    prec_stats = calculate_ensemble_statistics(prec_ensembles, prec_var)
    
    forecast_run.variables['TOT_PREC'] = VariableData(
        variable=prec_var,
        ensembles=prec_ensembles,
        statistics=prec_stats
    )
    
    # Create wind data (VMAX_10M)
    wind_var = get_variable_config('VMAX_10M')
    wind_ensembles = []
    
    for ens_id in range(1, 21):  # 20 ensembles
        # Generate realistic wind data
        base_wind = 5 + np.random.normal(0, 2, len(times))
        base_wind = np.maximum(base_wind, 0)  # No negative wind
        
        ensemble = EnsembleMember(
            ensemble_id=f"{ens_id:02d}",
            times=times,
            values=base_wind.tolist()
        )
        wind_ensembles.append(ensemble)
    
    # Calculate statistics
    wind_stats = calculate_ensemble_statistics(wind_ensembles, wind_var)
    
    forecast_run.variables['VMAX_10M'] = VariableData(
        variable=wind_var,
        ensembles=wind_ensembles,
        statistics=wind_stats
    )
    
    return forecast_run


def main():
    """Create demonstration weather data structure"""
    print("🎯 Creating demonstration weather data structure...")
    
    # Create demo forecast run
    forecast_run = create_demo_forecast_run()
    print(f"✅ Created forecast run: {forecast_run.run_time}")
    print(f"   Location: {forecast_run.location.name}")
    print(f"   Variables: {list(forecast_run.variables.keys())}")
    
    for var_id, var_data in forecast_run.variables.items():
        print(f"   {var_id}: {len(var_data.ensembles)} ensembles, {len(var_data.statistics.times)} time steps")
    
    # Save all outputs
    output_dir = Path('./data_demo')
    saved_files = save_all_outputs(
        forecast_run,
        output_dir,
        save_ensembles=True,
        save_statistics=True,
        save_netcdf=False
    )
    
    print(f"\n📁 Created demo weather structure in {output_dir}")
    print(f"✅ Master JSON: {saved_files['master_json']}")
    print(f"✅ Statistics files: {len(saved_files['statistics'])} files")
    print(f"✅ Ensemble files: {sum(len(files) for files in saved_files['ensembles'].values())} files")
    
    # Show directory structure
    weather_dir = output_dir / 'weather'
    print(f"\n📂 Directory structure:")
    
    for item in sorted(weather_dir.rglob('*')):
        relative_path = item.relative_to(weather_dir)
        indent = "  " * (len(relative_path.parts) - 1)
        if item.is_file():
            size_kb = item.stat().st_size / 1024
            print(f"{indent}📄 {relative_path.name} ({size_kb:.1f} KB)")
        else:
            print(f"{indent}📁 {relative_path.name}/")
    
    # Test master JSON compatibility
    master_json_path = weather_dir / f"{forecast_run.get_directory_name()}.json"
    if master_json_path.exists():
        with open(master_json_path) as f:
            master_data = json.load(f)
        
        print(f"\n🔍 Master JSON structure:")
        print(f"   run_time: {master_data['run_time']}")
        print(f"   location: {master_data['location']}")
        print(f"   coordinates: {master_data['coordinates']}")
        print(f"   variables: {list(master_data['variables'].keys())}")
        
        # Check derived variables
        has_derived = any(var.startswith('TOT_PREC_') for var in master_data['variables'].keys())
        print(f"   derived variables: {'✅' if has_derived else '❌'}")
        
        print("\n🎉 Demo weather data structure created successfully!")
        print(f"Frontend can now consume: {master_json_path}")
        
        return True
    
    return False


if __name__ == '__main__':
    success = main()
    if success:
        print("\n✅ Demo completed successfully!")
    else:
        print("\n❌ Demo failed!")