#!/usr/bin/env python3
"""
Data Input/Output Utilities
===========================

Functions for saving weather data in various formats.
"""

import json
import numpy as np
import xarray as xr
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from .models import (ForecastRun, VariableData, EnsembleMember, 
                    WeatherDataEncoder, save_json, get_variable_config)
from .statistics import calculate_derived_precipitation_variables

logger = logging.getLogger(__name__)


class OutputManager:
    """Manages output directory structure and file saving"""
    
    def __init__(self, base_output_dir: Path):
        self.base_output_dir = Path(base_output_dir)
        self.weather_dir = self.base_output_dir / 'weather'
        self.weather_dir.mkdir(parents=True, exist_ok=True)
    
    def get_forecast_dir(self, forecast_run: ForecastRun) -> Path:
        """Get directory path for a forecast run"""
        return self.weather_dir / forecast_run.get_directory_name()
    
    def create_forecast_dir(self, forecast_run: ForecastRun) -> Path:
        """Create and return directory for forecast run"""
        forecast_dir = self.get_forecast_dir(forecast_run)
        forecast_dir.mkdir(parents=True, exist_ok=True)
        return forecast_dir


def save_individual_ensemble_files(forecast_run: ForecastRun, 
                                 output_manager: OutputManager) -> Dict[str, List[Path]]:
    """
    Save individual ensemble JSON files.
    
    Args:
        forecast_run: Complete forecast run data
        output_manager: Output directory manager
    
    Returns:
        Dictionary mapping variable_id to list of saved file paths
    """
    forecast_dir = output_manager.create_forecast_dir(forecast_run)
    saved_files = {}
    
    for var_id, var_data in forecast_run.variables.items():
        if not var_data.ensembles:
            logger.warning(f"No ensemble data for variable {var_id}")
            continue
        
        variable_files = []
        
        for ensemble in var_data.ensembles:
            # Create filename: TOT_PREC_ensemble_01.json
            filename = f"{var_id}_ensemble_{ensemble.ensemble_id}.json"
            filepath = forecast_dir / filename
            
            # Create ensemble JSON data
            ensemble_data = ensemble.to_dict()
            ensemble_data.update({
                'variable': var_data.variable.name,
                'unit': var_data.variable.unit,
                'run_time': forecast_run.run_time,
                'location': forecast_run.location.name,
                'coordinates': forecast_run.location.coordinates
            })
            
            # Save file
            save_json(ensemble_data, filepath)
            variable_files.append(filepath)
            
        saved_files[var_id] = variable_files
        logger.info(f"Saved {len(variable_files)} ensemble files for {var_id}")
    
    return saved_files


def save_statistics_files(forecast_run: ForecastRun,
                        output_manager: OutputManager) -> Dict[str, Path]:
    """
    Save statistics JSON files for each variable.
    
    Args:
        forecast_run: Complete forecast run data
        output_manager: Output directory manager
    
    Returns:
        Dictionary mapping variable_id to saved file path
    """
    forecast_dir = output_manager.create_forecast_dir(forecast_run)
    saved_files = {}
    
    for var_id, var_data in forecast_run.variables.items():
        if not var_data.statistics:
            logger.warning(f"No statistics data for variable {var_id}")
            continue
        
        # Create filename: TOT_PREC_statistics.json
        filename = f"{var_id}_statistics.json"
        filepath = forecast_dir / filename
        
        # Create statistics JSON data
        stats_data = var_data.to_statistics_dict()
        stats_data.update({
            'run_time': forecast_run.run_time,
            'location': forecast_run.location.name,
            'coordinates': forecast_run.location.coordinates
        })
        
        # Save file
        save_json(stats_data, filepath)
        saved_files[var_id] = filepath
        
    logger.info(f"Saved statistics files for {len(saved_files)} variables")
    return saved_files


def create_master_json(forecast_run: ForecastRun,
                      output_manager: OutputManager) -> Path:
    """
    Create master JSON file with all forecast data for frontend.
    
    Args:
        forecast_run: Complete forecast run data
        output_manager: Output directory manager
    
    Returns:
        Path to saved master JSON file
    """
    forecast_dir = output_manager.create_forecast_dir(forecast_run)
    master_filepath = forecast_dir / 'forecast_master.json'
    
    # Start with base master JSON
    master_data = forecast_run.to_master_json()
    
    # Add derived precipitation variables if we have TOT_PREC
    if 'TOT_PREC' in forecast_run.variables and forecast_run.variables['TOT_PREC'].statistics:
        try:
            tot_prec_stats = forecast_run.variables['TOT_PREC'].statistics
            derived_vars = calculate_derived_precipitation_variables(tot_prec_stats)
            
            # Add derived variables to master JSON
            for derived_var_id, derived_stats in derived_vars.items():
                master_data['variables'][derived_var_id] = {
                    'name': f"Derived {derived_var_id.replace('_', ' ')}",
                    'unit': 'mm' if 'ACCUM' in derived_var_id or '1H' in derived_var_id else 'mm/h',
                    'num_ensembles': derived_stats.num_ensembles,
                    **derived_stats.to_dict()
                }
                
            logger.info(f"Added {len(derived_vars)} derived variables to master JSON")
            
        except Exception as e:
            logger.error(f"Error calculating derived variables: {e}")
    
    # Save master file
    save_json(master_data, master_filepath)
    
    # Also save as the main forecast JSON in weather/ directory
    main_forecast_file = output_manager.weather_dir / f"{forecast_run.get_directory_name()}.json"
    save_json(master_data, main_forecast_file)
    
    # Create/update latest.json symlink
    latest_json = output_manager.weather_dir / 'latest.json'
    try:
        # Remove existing symlink if it exists
        if latest_json.is_symlink() or latest_json.exists():
            latest_json.unlink()
        
        # Create new symlink (or copy on Windows)
        try:
            latest_json.symlink_to(main_forecast_file.name)
        except OSError:
            # Fallback: copy the file instead of symlinking
            save_json(master_data, latest_json)
            
    except Exception as e:
        logger.warning(f"Could not create latest.json: {e}")
    
    logger.info(f"Created master JSON: {master_filepath}")
    return master_filepath


def save_netcdf_backup(forecast_run: ForecastRun,
                      output_manager: OutputManager) -> Optional[Path]:
    """
    Save NetCDF backup file for compatibility.
    
    Args:
        forecast_run: Complete forecast run data  
        output_manager: Output directory manager
    
    Returns:
        Path to saved NetCDF file or None if save failed
    """
    try:
        # Create NetCDF filename
        netcdf_filename = f"bratislava_{forecast_run.run_time.replace(':', '').replace('-', '')}_{int(datetime.now().timestamp())}.nc"
        netcdf_dir = output_manager.base_output_dir / 'bratislava'
        netcdf_dir.mkdir(parents=True, exist_ok=True)
        netcdf_filepath = netcdf_dir / netcdf_filename
        
        # Create xarray dataset
        datasets = []
        
        for var_id, var_data in forecast_run.variables.items():
            if not var_data.statistics:
                continue
                
            stats = var_data.statistics
            variable = var_data.variable
            
            # Create time coordinate
            time_coord = xr.DataArray(
                stats.times,
                dims=['step'],
                name='step',
                attrs={'long_name': 'forecast_step', 'units': 'ISO8601'}
            )
            
            # Create data variables for all statistics
            data_vars = {}
            
            # Basic statistics
            for stat_name in ['mean', 'median', 'std', 'min', 'max']:
                data_vars[f'{variable.grib_shortName}_{stat_name}'] = xr.DataArray(
                    getattr(stats, stat_name),
                    dims=['step'],
                    coords={'step': time_coord},
                    attrs={
                        'long_name': f'{variable.name} {stat_name}',
                        'units': variable.unit
                    }
                )
            
            # Percentiles
            for p_key, p_values in stats.percentiles.items():
                data_vars[f'{variable.grib_shortName}_p{p_key}'] = xr.DataArray(
                    p_values,
                    dims=['step'],
                    coords={'step': time_coord},
                    attrs={
                        'long_name': f'{variable.name} {p_key}th percentile',
                        'units': variable.unit
                    }
                )
            
            # Create dataset for this variable
            var_ds = xr.Dataset(
                data_vars,
                coords={'step': time_coord},
                attrs={
                    'variable_id': var_id,
                    'variable_name': variable.name,
                    'num_ensembles': stats.num_ensembles
                }
            )
            
            datasets.append(var_ds)
        
        if not datasets:
            logger.warning("No data to save to NetCDF")
            return None
        
        # Merge datasets
        if len(datasets) == 1:
            combined_ds = datasets[0]
        else:
            combined_ds = xr.merge(datasets, compat='no_conflicts')
        
        # Add global attributes
        combined_ds.attrs.update({
            'run_time': forecast_run.run_time,
            'location': forecast_run.location.name,
            'actual_coordinates': f"{forecast_run.location.lat:.4f}°N, {forecast_run.location.lon:.4f}°E",
            'coordinate_accuracy': f"{forecast_run.location.distance_km:.2f} km",
            'processed_at': forecast_run.processed_at or datetime.now().isoformat(),
            'data_source': 'DWD ICON-D2-RUC-EPS',
            'processing_pipeline': 'modular_bratislava_pipeline'
        })
        
        # Save to NetCDF
        combined_ds.to_netcdf(netcdf_filepath)
        combined_ds.close()
        
        logger.info(f"Saved NetCDF backup: {netcdf_filepath}")
        return netcdf_filepath
        
    except Exception as e:
        logger.error(f"Error saving NetCDF backup: {e}")
        return None


def save_all_outputs(forecast_run: ForecastRun,
                    output_dir: Path,
                    save_ensembles: bool = True,
                    save_statistics: bool = True,
                    save_netcdf: bool = False) -> Dict[str, Any]:
    """
    Save all output files for a forecast run.
    
    Args:
        forecast_run: Complete forecast run data
        output_dir: Base output directory
        save_ensembles: Whether to save individual ensemble files
        save_statistics: Whether to save statistics files
        save_netcdf: Whether to save NetCDF backup
    
    Returns:
        Dictionary with information about saved files
    """
    output_manager = OutputManager(output_dir)
    saved_files = {
        'ensembles': {},
        'statistics': {},
        'master_json': None,
        'netcdf_backup': None
    }
    
    try:
        # Save individual ensemble files
        if save_ensembles:
            saved_files['ensembles'] = save_individual_ensemble_files(
                forecast_run, output_manager
            )
        
        # Save statistics files
        if save_statistics:
            saved_files['statistics'] = save_statistics_files(
                forecast_run, output_manager
            )
        
        # Create master JSON (always create this for frontend)
        saved_files['master_json'] = create_master_json(
            forecast_run, output_manager
        )
        
        # Save NetCDF backup if requested
        if save_netcdf:
            saved_files['netcdf_backup'] = save_netcdf_backup(
                forecast_run, output_manager
            )
        
        logger.info(f"Successfully saved all outputs for run {forecast_run.run_time}")
        
    except Exception as e:
        logger.error(f"Error saving outputs: {e}")
        raise
    
    return saved_files


def cleanup_old_forecasts(weather_dir: Path, 
                        keep_latest: int = 10,
                        dry_run: bool = True) -> List[Path]:
    """
    Clean up old forecast directories.
    
    Args:
        weather_dir: Weather data directory
        keep_latest: Number of latest forecasts to keep
        dry_run: If True, only report what would be deleted
    
    Returns:
        List of directories that were (or would be) deleted
    """
    if not weather_dir.exists():
        return []
    
    # Find all forecast directories
    forecast_dirs = [d for d in weather_dir.iterdir() 
                    if d.is_dir() and d.name.startswith('forecast_')]
    
    # Sort by modification time (newest first)
    forecast_dirs.sort(key=lambda d: d.stat().st_mtime, reverse=True)
    
    # Determine which to delete (keep the latest N)
    to_delete = forecast_dirs[keep_latest:]
    
    if dry_run:
        logger.info(f"Would delete {len(to_delete)} old forecast directories")
        for d in to_delete:
            logger.info(f"  Would delete: {d}")
    else:
        logger.info(f"Deleting {len(to_delete)} old forecast directories")
        for d in to_delete:
            try:
                import shutil
                shutil.rmtree(d)
                logger.info(f"Deleted: {d}")
            except Exception as e:
                logger.error(f"Error deleting {d}: {e}")
    
    return to_delete


def verify_output_structure(forecast_run: ForecastRun, 
                          output_dir: Path) -> Dict[str, bool]:
    """
    Verify that all expected output files exist.
    
    Args:
        forecast_run: Forecast run to verify
        output_dir: Base output directory
    
    Returns:
        Dictionary with verification results
    """
    output_manager = OutputManager(output_dir)
    forecast_dir = output_manager.get_forecast_dir(forecast_run)
    
    verification = {
        'forecast_directory_exists': forecast_dir.exists(),
        'master_json_exists': (forecast_dir / 'forecast_master.json').exists(),
        'main_json_exists': (output_manager.weather_dir / f"{forecast_run.get_directory_name()}.json").exists(),
        'ensemble_files_exist': True,
        'statistics_files_exist': True
    }
    
    if not verification['forecast_directory_exists']:
        return verification
    
    # Check ensemble files
    for var_id, var_data in forecast_run.variables.items():
        for ensemble in var_data.ensembles:
            filename = f"{var_id}_ensemble_{ensemble.ensemble_id}.json"
            if not (forecast_dir / filename).exists():
                verification['ensemble_files_exist'] = False
                break
        if not verification['ensemble_files_exist']:
            break
    
    # Check statistics files
    for var_id in forecast_run.variables.keys():
        filename = f"{var_id}_statistics.json"
        if not (forecast_dir / filename).exists():
            verification['statistics_files_exist'] = False
            break
    
    return verification