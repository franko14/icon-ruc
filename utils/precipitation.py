"""
Precipitation-specific processing utilities for ICON-D2-RUC-EPS
"""
import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime
import sys
sys.path.append('..')
from config import *

class PrecipitationProcessor:
    """
    Class for processing accumulated precipitation data from ICON-D2-RUC-EPS
    """
    
    def __init__(self):
        self.time_aggregations = TIME_AGGREGATIONS
        self.percentiles = PERCENTILES
        self.thresholds = PRECIP_THRESHOLDS
    
    def deaccumulate(self, data, time_dim='step'):
        """
        Convert accumulated precipitation to interval precipitation rates.
        
        Args:
            data (xr.Dataset): Dataset with accumulated precipitation
            time_dim (str): Name of time dimension
            
        Returns:
            xr.Dataset: Dataset with deaccumulated precipitation rates
        """
        print("Deaccumulating precipitation data...")
        
        # Create a copy to avoid modifying original
        data_deacc = data.copy()
        
        # Calculate differences between time steps for precipitation
        if 'tp' in data_deacc.data_vars:
            # Calculate precipitation rate (difference between consecutive time steps)
            precip_diff = data_deacc.tp.diff(dim=time_dim)
            
            # Handle first timestep - assume it's the accumulated value at first step
            first_step = data_deacc.tp.isel({time_dim: 0})
            
            # Concatenate first step with differences
            precip_rate = xr.concat([first_step, precip_diff], dim=time_dim)
            
            # Replace accumulated values with rates
            data_deacc['tp'] = precip_rate
            
            # Update attributes
            if 'units' in data_deacc.tp.attrs:
                # Convert from total accumulation to rate
                original_unit = data_deacc.tp.attrs['units']
                if 'kg m-2' in original_unit:
                    data_deacc.tp.attrs['units'] = 'kg m-2 per 15min'
                    data_deacc.tp.attrs['long_name'] = 'Precipitation rate (15min intervals)'
            
            # Set negative values to zero (can occur due to model spin-up issues)
            data_deacc['tp'] = data_deacc.tp.where(data_deacc.tp >= 0, 0)
            
            print(f"Deaccumulated precipitation data")
            print(f"  Original max: {float(data.tp.max()):.4f} kg m-2")
            print(f"  Deacc max rate: {float(data_deacc.tp.max()):.4f} kg m-2 per 15min")
        
        return data_deacc
    
    def aggregate_time(self, data, period, time_dim='step'):
        """
        Aggregate precipitation to different time periods.
        
        Args:
            data (xr.Dataset): Dataset with precipitation data
            period (str): Target aggregation period ('1h', '3h', etc.)
            time_dim (str): Name of time dimension
            
        Returns:
            xr.Dataset: Time-aggregated dataset
        """
        if period == '15min':
            # No aggregation needed
            return data
        
        if period not in self.time_aggregations:
            print(f"Unknown aggregation period: {period}")
            print(f"Available periods: {list(self.time_aggregations.keys())}")
            return data
        
        print(f"Aggregating precipitation to {period} intervals...")
        
        # Convert step coordinate to datetime for resampling
        if data[time_dim].dtype.kind not in ['M', 'datetime64']:
            # Assume step is in hours, convert to timedelta
            step_hours = data[time_dim]
            # Create datetime index starting from 0
            time_index = pd.to_timedelta(step_hours, unit='h')
            data = data.assign_coords({time_dim: time_index})
        
        # Resample and sum precipitation
        resampled = data.resample({time_dim: self.time_aggregations[period]}).sum()
        
        # Update step coordinate back to hours
        new_steps = np.arange(len(resampled[time_dim])) * self._period_to_hours(period)
        resampled = resampled.assign_coords({time_dim: new_steps})
        
        # Update units
        if 'tp' in resampled.data_vars:
            if 'units' in resampled.tp.attrs:
                if 'per 15min' in resampled.tp.attrs['units']:
                    resampled.tp.attrs['units'] = f"kg m-2 per {period}"
                    resampled.tp.attrs['long_name'] = f'Precipitation rate ({period} intervals)'
        
        print(f"Aggregated to {period}: {len(resampled[time_dim])} time steps")
        return resampled
    
    def _period_to_hours(self, period):
        """Convert period string to hours"""
        period_hours = {
            '15min': 0.25,
            '1h': 1.0,
            '3h': 3.0,
            '6h': 6.0,
            '12h': 12.0,
            '24h': 24.0
        }
        return period_hours.get(period, 1.0)
    
    def calculate_percentiles(self, data, ensemble_dim='ensemble'):
        """
        Calculate ensemble percentiles for precipitation.
        
        Args:
            data (xr.Dataset): Dataset with ensemble dimension
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            xr.Dataset: Dataset with percentile variables
        """
        print("Calculating ensemble percentiles...")
        
        if ensemble_dim not in data.dims:
            print(f"Warning: {ensemble_dim} dimension not found in dataset")
            return data
        
        percentile_data = {}
        
        # Calculate percentiles for precipitation
        if 'tp' in data.data_vars:
            for p in self.percentiles:
                var_name = f'tp_p{p:02d}'
                percentile_data[var_name] = data.tp.quantile(p/100.0, dim=ensemble_dim)
                percentile_data[var_name].attrs = {
                    'long_name': f'Precipitation {p}th percentile',
                    'units': data.tp.attrs.get('units', 'kg m-2')
                }
        
        # Create new dataset with percentiles
        result_data = data.copy()
        for var_name, var_data in percentile_data.items():
            result_data[var_name] = var_data
        
        print(f"Added {len(percentile_data)} percentile variables")
        return result_data
    
    def calculate_ensemble_statistics(self, data, ensemble_dim='ensemble'):
        """
        Calculate comprehensive ensemble statistics.
        
        Args:
            data (xr.Dataset): Dataset with ensemble dimension
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            xr.Dataset: Dataset with statistical variables
        """
        print("Calculating ensemble statistics...")
        
        if ensemble_dim not in data.dims:
            print(f"Warning: {ensemble_dim} dimension not found in dataset")
            return data
        
        stats_data = {}
        
        if 'tp' in data.data_vars:
            # Basic statistics
            stats_data['tp_mean'] = data.tp.mean(dim=ensemble_dim)
            stats_data['tp_median'] = data.tp.median(dim=ensemble_dim)
            stats_data['tp_std'] = data.tp.std(dim=ensemble_dim)
            stats_data['tp_min'] = data.tp.min(dim=ensemble_dim)
            stats_data['tp_max'] = data.tp.max(dim=ensemble_dim)
            
            # Interquartile range
            q25 = data.tp.quantile(0.25, dim=ensemble_dim)
            q75 = data.tp.quantile(0.75, dim=ensemble_dim)
            stats_data['tp_iqr'] = q75 - q25
            
            # Coefficient of variation (std/mean)
            stats_data['tp_cv'] = stats_data['tp_std'] / stats_data['tp_mean'].where(
                stats_data['tp_mean'] > 0
            )
            
            # Add attributes
            stats_attrs = {
                'tp_mean': {'long_name': 'Ensemble mean precipitation', 'units': data.tp.attrs.get('units', 'kg m-2')},
                'tp_median': {'long_name': 'Ensemble median precipitation', 'units': data.tp.attrs.get('units', 'kg m-2')},
                'tp_std': {'long_name': 'Ensemble standard deviation', 'units': data.tp.attrs.get('units', 'kg m-2')},
                'tp_min': {'long_name': 'Ensemble minimum', 'units': data.tp.attrs.get('units', 'kg m-2')},
                'tp_max': {'long_name': 'Ensemble maximum', 'units': data.tp.attrs.get('units', 'kg m-2')},
                'tp_iqr': {'long_name': 'Interquartile range', 'units': data.tp.attrs.get('units', 'kg m-2')},
                'tp_cv': {'long_name': 'Coefficient of variation', 'units': 'dimensionless'}
            }
            
            for var_name, var_data in stats_data.items():
                var_data.attrs = stats_attrs.get(var_name, {})
        
        # Create new dataset with statistics
        result_data = data.copy()
        for var_name, var_data in stats_data.items():
            result_data[var_name] = var_data
        
        print(f"Added {len(stats_data)} statistical variables")
        return result_data
    
    def probability_exceedance(self, data, thresholds=None, ensemble_dim='ensemble'):
        """
        Calculate probability of exceeding precipitation thresholds.
        
        Args:
            data (xr.Dataset): Dataset with ensemble dimension
            thresholds (list): List of precipitation thresholds
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            xr.Dataset: Dataset with probability variables
        """
        if thresholds is None:
            thresholds = self.thresholds
        
        print(f"Calculating exceedance probabilities for {len(thresholds)} thresholds...")
        
        if ensemble_dim not in data.dims:
            print(f"Warning: {ensemble_dim} dimension not found in dataset")
            return data
        
        prob_data = {}
        
        if 'tp' in data.data_vars:
            for threshold in thresholds:
                var_name = f'tp_prob_{threshold:g}'.replace('.', 'p')
                
                # Calculate probability as fraction of ensemble members exceeding threshold
                prob_data[var_name] = (data.tp > threshold).mean(dim=ensemble_dim)
                
                prob_data[var_name].attrs = {
                    'long_name': f'Probability of precipitation > {threshold} kg/m²',
                    'units': 'fraction',
                    'threshold': threshold
                }
        
        # Create new dataset with probabilities
        result_data = data.copy()
        for var_name, var_data in prob_data.items():
            result_data[var_name] = var_data
        
        print(f"Added {len(prob_data)} probability variables")
        return result_data
    
    def convert_units(self, data, target_unit='mm/h', time_interval_minutes=15):
        """
        Convert precipitation units.
        
        Args:
            data (xr.Dataset): Dataset with precipitation data
            target_unit (str): Target unit ('mm/h', 'mm', 'kg/m2')
            time_interval_minutes (int): Time interval of the data in minutes
            
        Returns:
            xr.Dataset: Dataset with converted units
        """
        print(f"Converting precipitation units to {target_unit}...")
        
        result_data = data.copy()
        
        if 'tp' in result_data.data_vars:
            current_unit = result_data.tp.attrs.get('units', 'kg m-2')
            
            # Convert kg m-2 to mm (1 kg/m² = 1 mm)
            if 'kg m-2' in current_unit or 'kg/m' in current_unit:
                if target_unit == 'mm':
                    # Direct conversion
                    result_data.tp.attrs['units'] = 'mm'
                    result_data.tp.attrs['long_name'] = 'Precipitation amount'
                    
                elif target_unit == 'mm/h':
                    # Convert to hourly rate
                    conversion_factor = 60.0 / time_interval_minutes
                    result_data['tp'] = result_data.tp * conversion_factor
                    result_data.tp.attrs['units'] = 'mm/h'
                    result_data.tp.attrs['long_name'] = 'Precipitation rate'
        
        # Update other precipitation variables if they exist
        for var_name in result_data.data_vars:
            if var_name.startswith('tp_') and var_name != 'tp':
                if 'kg m-2' in result_data[var_name].attrs.get('units', ''):
                    if target_unit == 'mm':
                        result_data[var_name].attrs['units'] = 'mm'
                    elif target_unit == 'mm/h':
                        conversion_factor = 60.0 / time_interval_minutes
                        result_data[var_name] = result_data[var_name] * conversion_factor
                        result_data[var_name].attrs['units'] = 'mm/h'
        
        print(f"Converted units from {current_unit} to {target_unit}")
        return result_data
    
    def create_precipitation_summary(self, data):
        """
        Create a summary of precipitation statistics.
        
        Args:
            data (xr.Dataset): Dataset with precipitation data
            
        Returns:
            dict: Summary statistics
        """
        summary = {}
        
        if 'tp' in data.data_vars:
            tp_data = data.tp
            
            summary['total_timesteps'] = tp_data.sizes.get('step', 0)
            summary['total_runs'] = tp_data.sizes.get('run_time', 0) 
            summary['total_ensembles'] = tp_data.sizes.get('ensemble', 0)
            
            # Overall statistics
            summary['global_min'] = float(tp_data.min())
            summary['global_max'] = float(tp_data.max())
            summary['global_mean'] = float(tp_data.mean())
            summary['global_std'] = float(tp_data.std())
            
            # Units
            summary['units'] = tp_data.attrs.get('units', 'unknown')
            
            # Dimensions
            summary['dimensions'] = list(tp_data.dims)
            summary['shape'] = list(tp_data.shape)
            
            # Time range
            if 'step' in tp_data.dims:
                steps = tp_data.step
                summary['forecast_range'] = f"{float(steps.min()):.1f}h to {float(steps.max()):.1f}h"
            
            # Count of non-zero precipitation
            non_zero = (tp_data > 0).sum()
            total_points = tp_data.size
            summary['non_zero_fraction'] = float(non_zero / total_points)
            
        return summary
    
    def print_summary(self, summary):
        """Print precipitation summary in formatted way"""
        print("\nPRECIPITATION DATA SUMMARY")
        print("=" * 30)
        
        print(f"Dimensions: {' × '.join(map(str, summary.get('shape', [])))}")
        print(f"Variables: {summary.get('dimensions', [])}")
        print(f"Units: {summary.get('units', 'unknown')}")
        
        if summary.get('forecast_range'):
            print(f"Forecast range: {summary['forecast_range']}")
        
        print(f"\nStatistics:")
        print(f"  Min: {summary.get('global_min', 0):.4f}")
        print(f"  Max: {summary.get('global_max', 0):.4f}") 
        print(f"  Mean: {summary.get('global_mean', 0):.4f}")
        print(f"  Std: {summary.get('global_std', 0):.4f}")
        
        print(f"\nNon-zero precipitation: {summary.get('non_zero_fraction', 0)*100:.1f}% of grid points")