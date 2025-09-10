"""
Wind Speed processing utilities for ICON-D2-RUC-EPS VMAX_10M data
"""
import numpy as np
import xarray as xr
import pandas as pd
from datetime import datetime
import sys
sys.path.append('..')
from config import VARIABLES_CONFIG, PERCENTILES

class WindSpeedProcessor:
    """
    Class for processing instantaneous wind speed data from ICON-D2-RUC-EPS VMAX_10M
    """
    
    def __init__(self):
        self.config = VARIABLES_CONFIG['VMAX_10M']
        self.time_aggregations = self.config['aggregations']
        self.percentiles = self.config['percentiles']
        self.thresholds = self.config['thresholds']
    
    def calculate_wind_statistics(self, data, ensemble_dim='ensemble'):
        """
        Calculate wind-specific statistics including gust probabilities and calm periods.
        
        Args:
            data (xr.Dataset): Dataset with wind speed data (instantaneous values)
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            xr.Dataset: Dataset with wind-specific statistical variables
        """
        print("Calculating wind speed ensemble statistics...")
        
        if ensemble_dim not in data.dims:
            print(f"Warning: {ensemble_dim} dimension not found in dataset")
            return data
        
        stats_data = {}
        
        if 'vmax_10m' in data.data_vars:
            wind = data.vmax_10m
            
            # Basic statistics
            stats_data['vmax_10m_mean'] = wind.mean(dim=ensemble_dim)
            stats_data['vmax_10m_median'] = wind.median(dim=ensemble_dim)
            stats_data['vmax_10m_std'] = wind.std(dim=ensemble_dim)
            stats_data['vmax_10m_min'] = wind.min(dim=ensemble_dim)
            stats_data['vmax_10m_max'] = wind.max(dim=ensemble_dim)
            
            # Interquartile range
            q25 = wind.quantile(0.25, dim=ensemble_dim)
            q75 = wind.quantile(0.75, dim=ensemble_dim)
            stats_data['vmax_10m_iqr'] = q75 - q25
            
            # Coefficient of variation (std/mean)
            stats_data['vmax_10m_cv'] = stats_data['vmax_10m_std'] / stats_data['vmax_10m_mean'].where(
                stats_data['vmax_10m_mean'] > 0
            )
            
            # Wind-specific statistics
            # Gust factor (max/mean)
            stats_data['vmax_10m_gust_factor'] = stats_data['vmax_10m_max'] / stats_data['vmax_10m_mean'].where(
                stats_data['vmax_10m_mean'] > 0
            )
            
            # Calm probability (< 3 m/s)
            stats_data['vmax_10m_calm_prob'] = (wind < 3.0).mean(dim=ensemble_dim)
            
            # Strong wind probability (> 10 m/s)
            stats_data['vmax_10m_strong_prob'] = (wind > 10.0).mean(dim=ensemble_dim)
            
            # Very strong wind probability (> 15 m/s)
            stats_data['vmax_10m_very_strong_prob'] = (wind > 15.0).mean(dim=ensemble_dim)
            
            # Extreme wind probability (> 25 m/s)
            stats_data['vmax_10m_extreme_prob'] = (wind > 25.0).mean(dim=ensemble_dim)
            
            # Add attributes
            units = wind.attrs.get('units', 'm/s')
            stats_attrs = {
                'vmax_10m_mean': {'long_name': 'Ensemble mean wind speed', 'units': units},
                'vmax_10m_median': {'long_name': 'Ensemble median wind speed', 'units': units},
                'vmax_10m_std': {'long_name': 'Ensemble standard deviation', 'units': units},
                'vmax_10m_min': {'long_name': 'Ensemble minimum wind speed', 'units': units},
                'vmax_10m_max': {'long_name': 'Ensemble maximum wind speed', 'units': units},
                'vmax_10m_iqr': {'long_name': 'Interquartile range', 'units': units},
                'vmax_10m_cv': {'long_name': 'Coefficient of variation', 'units': 'dimensionless'},
                'vmax_10m_gust_factor': {'long_name': 'Gust factor (max/mean)', 'units': 'dimensionless'},
                'vmax_10m_calm_prob': {'long_name': 'Calm wind probability (< 3 m/s)', 'units': 'fraction'},
                'vmax_10m_strong_prob': {'long_name': 'Strong wind probability (> 10 m/s)', 'units': 'fraction'},
                'vmax_10m_very_strong_prob': {'long_name': 'Very strong wind probability (> 15 m/s)', 'units': 'fraction'},
                'vmax_10m_extreme_prob': {'long_name': 'Extreme wind probability (> 25 m/s)', 'units': 'fraction'}
            }
            
            for var_name, var_data in stats_data.items():
                var_data.attrs = stats_attrs.get(var_name, {})
        
        # Create new dataset with statistics
        result_data = data.copy()
        for var_name, var_data in stats_data.items():
            result_data[var_name] = var_data
        
        print(f"Added {len(stats_data)} wind statistical variables")
        return result_data
    
    def calculate_percentiles(self, data, ensemble_dim='ensemble'):
        """
        Calculate ensemble percentiles for wind speed.
        
        Args:
            data (xr.Dataset): Dataset with ensemble dimension
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            xr.Dataset: Dataset with percentile variables
        """
        print("Calculating wind speed ensemble percentiles...")
        
        if ensemble_dim not in data.dims:
            print(f"Warning: {ensemble_dim} dimension not found in dataset")
            return data
        
        percentile_data = {}
        
        # Calculate percentiles for wind speed
        if 'vmax_10m' in data.data_vars:
            for p in self.percentiles:
                var_name = f'vmax_10m_p{p:02d}'
                percentile_data[var_name] = data.vmax_10m.quantile(p/100.0, dim=ensemble_dim)
                percentile_data[var_name].attrs = {
                    'long_name': f'Wind speed {p}th percentile',
                    'units': data.vmax_10m.attrs.get('units', 'm/s')
                }
        
        # Create new dataset with percentiles
        result_data = data.copy()
        for var_name, var_data in percentile_data.items():
            result_data[var_name] = var_data
        
        print(f"Added {len(percentile_data)} percentile variables")
        return result_data
    
    def probability_exceedance(self, data, thresholds=None, ensemble_dim='ensemble'):
        """
        Calculate probability of exceeding wind speed thresholds.
        
        Args:
            data (xr.Dataset): Dataset with ensemble dimension
            thresholds (list): List of wind speed thresholds (m/s)
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            xr.Dataset: Dataset with probability variables
        """
        if thresholds is None:
            thresholds = self.thresholds
        
        print(f"Calculating wind speed exceedance probabilities for {len(thresholds)} thresholds...")
        
        if ensemble_dim not in data.dims:
            print(f"Warning: {ensemble_dim} dimension not found in dataset")
            return data
        
        prob_data = {}
        
        if 'vmax_10m' in data.data_vars:
            for threshold in thresholds:
                var_name = f'vmax_10m_prob_{threshold:g}'.replace('.', 'p')
                
                # Calculate probability as fraction of ensemble members exceeding threshold
                prob_data[var_name] = (data.vmax_10m > threshold).mean(dim=ensemble_dim)
                
                prob_data[var_name].attrs = {
                    'long_name': f'Probability of wind speed > {threshold} m/s',
                    'units': 'fraction',
                    'threshold': threshold
                }
        
        # Create new dataset with probabilities
        result_data = data.copy()
        for var_name, var_data in prob_data.items():
            result_data[var_name] = var_data
        
        print(f"Added {len(prob_data)} probability variables")
        return result_data
    
    def aggregate_time(self, data, period, time_dim='step'):
        """
        Aggregate wind speed to different time periods using appropriate statistics.
        Wind speed uses maximum for aggregation (capturing peak gusts).
        
        Args:
            data (xr.Dataset): Dataset with wind speed data
            period (str): Target aggregation period ('1h', '3h', etc.)
            time_dim (str): Name of time dimension
            
        Returns:
            xr.Dataset: Time-aggregated dataset
        """
        if period == '15min':
            # No aggregation needed
            return data
        
        period_mapping = {
            '15min': '15T',
            '1h': '1H', 
            '3h': '3H',
            '6h': '6H'
        }
        
        if period not in period_mapping:
            print(f"Unknown aggregation period: {period}")
            print(f"Available periods: {list(period_mapping.keys())}")
            return data
        
        print(f"Aggregating wind speed to {period} intervals (using max)...")
        
        # Convert step coordinate to datetime for resampling
        if data[time_dim].dtype.kind not in ['M', 'datetime64']:
            # Assume step is in hours, convert to timedelta
            step_hours = data[time_dim]
            # Create datetime index starting from 0
            time_index = pd.to_timedelta(step_hours, unit='h')
            data = data.assign_coords({time_dim: time_index})
        
        # Resample and take maximum for wind speed (to capture gusts)
        resampled = data.resample({time_dim: period_mapping[period]}).max()
        
        # Update step coordinate back to hours
        new_steps = np.arange(len(resampled[time_dim])) * self._period_to_hours(period)
        resampled = resampled.assign_coords({time_dim: new_steps})
        
        # Update attributes
        if 'vmax_10m' in resampled.data_vars:
            if 'long_name' in resampled.vmax_10m.attrs:
                resampled.vmax_10m.attrs['long_name'] = f'Maximum wind speed ({period} intervals)'
        
        print(f"Aggregated to {period}: {len(resampled[time_dim])} time steps")
        return resampled
    
    def _period_to_hours(self, period):
        """Convert period string to hours"""
        period_hours = {
            '15min': 0.25,
            '1h': 1.0,
            '3h': 3.0,
            '6h': 6.0
        }
        return period_hours.get(period, 1.0)
    
    def classify_wind_conditions(self, data):
        """
        Classify wind conditions based on Beaufort scale categories.
        
        Args:
            data (xr.Dataset): Dataset with wind speed data
            
        Returns:
            xr.Dataset: Dataset with wind classification variables
        """
        print("Classifying wind conditions using Beaufort scale...")
        
        if 'vmax_10m' not in data.data_vars:
            print("Warning: vmax_10m variable not found in dataset")
            return data
        
        wind = data.vmax_10m
        result_data = data.copy()
        
        # Beaufort scale classifications (simplified)
        # 0-3: Calm to Light breeze
        result_data['wind_calm_light'] = (wind <= 5.4).astype(int)
        
        # 4-5: Moderate to Fresh breeze  
        result_data['wind_moderate_fresh'] = ((wind > 5.4) & (wind <= 10.7)).astype(int)
        
        # 6-7: Strong to Near gale
        result_data['wind_strong_gale'] = ((wind > 10.7) & (wind <= 17.1)).astype(int)
        
        # 8-9: Gale to Strong gale
        result_data['wind_gale_strong'] = ((wind > 17.1) & (wind <= 24.4)).astype(int)
        
        # 10+: Storm conditions
        result_data['wind_storm'] = (wind > 24.4).astype(int)
        
        # Add attributes
        classifications = {
            'wind_calm_light': {'long_name': 'Calm to light breeze (0-5.4 m/s)', 'units': 'boolean'},
            'wind_moderate_fresh': {'long_name': 'Moderate to fresh breeze (5.4-10.7 m/s)', 'units': 'boolean'},
            'wind_strong_gale': {'long_name': 'Strong breeze to near gale (10.7-17.1 m/s)', 'units': 'boolean'},
            'wind_gale_strong': {'long_name': 'Gale to strong gale (17.1-24.4 m/s)', 'units': 'boolean'},
            'wind_storm': {'long_name': 'Storm conditions (>24.4 m/s)', 'units': 'boolean'}
        }
        
        for var_name, attrs in classifications.items():
            result_data[var_name].attrs = attrs
        
        print(f"Added {len(classifications)} wind classification variables")
        return result_data
    
    def calculate_wind_roses_data(self, data, ensemble_dim='ensemble'):
        """
        Calculate statistical data for wind roses (speed distributions).
        Note: VMAX_10M only provides speed, not direction.
        
        Args:
            data (xr.Dataset): Dataset with wind speed data
            ensemble_dim (str): Name of ensemble dimension
            
        Returns:
            dict: Wind speed distribution statistics
        """
        print("Calculating wind speed distribution for wind roses...")
        
        if 'vmax_10m' not in data.data_vars:
            return {}
        
        wind = data.vmax_10m
        
        # Speed bins for wind rose
        speed_bins = [0, 3, 6, 10, 15, 20, 25, 30, 100]  # m/s
        speed_labels = ['0-3', '3-6', '6-10', '10-15', '15-20', '20-25', '25-30', '30+']
        
        # Calculate distribution across ensemble and time
        if ensemble_dim in wind.dims:
            # Flatten ensemble and time dimensions
            wind_flat = wind.stack(samples=(ensemble_dim, 'step'))
        else:
            wind_flat = wind.stack(samples=['step'])
        
        # Calculate frequency in each speed bin
        speed_dist = {}
        for i, (low, high) in enumerate(zip(speed_bins[:-1], speed_bins[1:])):
            if i == len(speed_labels) - 1:  # Last bin
                mask = wind_flat >= low
            else:
                mask = (wind_flat >= low) & (wind_flat < high)
            
            speed_dist[speed_labels[i]] = float(mask.mean())
        
        return {
            'speed_distribution': speed_dist,
            'total_samples': int(wind_flat.sizes['samples']),
            'mean_wind_speed': float(wind_flat.mean()),
            'max_wind_speed': float(wind_flat.max()),
            'calm_fraction': float((wind_flat < 3).mean())
        }
    
    def create_wind_summary(self, data):
        """
        Create a summary of wind speed statistics.
        
        Args:
            data (xr.Dataset): Dataset with wind speed data
            
        Returns:
            dict: Summary statistics
        """
        summary = {}
        
        if 'vmax_10m' in data.data_vars:
            wind_data = data.vmax_10m
            
            summary['total_timesteps'] = wind_data.sizes.get('step', 0)
            summary['total_runs'] = wind_data.sizes.get('run_time', 0) 
            summary['total_ensembles'] = wind_data.sizes.get('ensemble', 0)
            
            # Overall statistics
            summary['global_min'] = float(wind_data.min())
            summary['global_max'] = float(wind_data.max())
            summary['global_mean'] = float(wind_data.mean())
            summary['global_std'] = float(wind_data.std())
            
            # Units
            summary['units'] = wind_data.attrs.get('units', 'm/s')
            
            # Dimensions
            summary['dimensions'] = list(wind_data.dims)
            summary['shape'] = list(wind_data.shape)
            
            # Time range
            if 'step' in wind_data.dims:
                steps = wind_data.step
                summary['forecast_range'] = f"{float(steps.min()):.1f}h to {float(steps.max()):.1f}h"
            
            # Wind-specific statistics
            summary['calm_fraction'] = float((wind_data < 3).mean())  # < 3 m/s
            summary['strong_wind_fraction'] = float((wind_data > 10).mean())  # > 10 m/s
            summary['gale_fraction'] = float((wind_data > 17).mean())  # > 17 m/s (gale force)
            summary['storm_fraction'] = float((wind_data > 25).mean())  # > 25 m/s (storm)
            
        return summary
    
    def print_summary(self, summary):
        """Print wind speed summary in formatted way"""
        print("\nWIND SPEED DATA SUMMARY")
        print("=" * 25)
        
        print(f"Dimensions: {' × '.join(map(str, summary.get('shape', [])))}")
        print(f"Variables: {summary.get('dimensions', [])}")
        print(f"Units: {summary.get('units', 'm/s')}")
        
        if summary.get('forecast_range'):
            print(f"Forecast range: {summary['forecast_range']}")
        
        print(f"\nStatistics:")
        print(f"  Min: {summary.get('global_min', 0):.1f} m/s")
        print(f"  Max: {summary.get('global_max', 0):.1f} m/s") 
        print(f"  Mean: {summary.get('global_mean', 0):.1f} m/s")
        print(f"  Std: {summary.get('global_std', 0):.1f} m/s")
        
        print(f"\nWind Conditions:")
        print(f"  Calm (< 3 m/s): {summary.get('calm_fraction', 0)*100:.1f}%")
        print(f"  Strong (> 10 m/s): {summary.get('strong_wind_fraction', 0)*100:.1f}%")
        print(f"  Gale (> 17 m/s): {summary.get('gale_fraction', 0)*100:.1f}%")
        print(f"  Storm (> 25 m/s): {summary.get('storm_fraction', 0)*100:.1f}%")