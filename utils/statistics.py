#!/usr/bin/env python3
"""
Statistical Processing Utilities
===============================

Functions for calculating ensemble statistics and derived weather variables.
"""

import numpy as np
from typing import List, Dict, Tuple, Optional
from datetime import datetime, timedelta
import logging

from .models import EnsembleMember, EnsembleStatistics, WeatherVariable

logger = logging.getLogger(__name__)


def deaccumulate_precipitation(data: List[float]) -> List[float]:
    """
    Convert accumulated precipitation to precipitation rates.
    
    Args:
        data: List of accumulated precipitation values
    
    Returns:
        List of precipitation rates (deaccumulated)
    """
    if len(data) == 0:
        return []
    
    # First value is the rate itself (not accumulated)
    # Subsequent values need deaccumulation
    deaccumulated = [data[0]]  # First value
    
    for i in range(1, len(data)):
        rate = data[i] - data[i-1]
        # Ensure non-negative rates (handle potential numerical issues)
        rate = max(0.0, rate)
        deaccumulated.append(rate)
    
    return deaccumulated


def calculate_ensemble_statistics(ensembles: List[EnsembleMember],
                                variable: WeatherVariable) -> EnsembleStatistics:
    """
    Calculate statistical metrics from ensemble data.
    
    Args:
        ensembles: List of ensemble members
        variable: Weather variable configuration
    
    Returns:
        EnsembleStatistics object
    """
    if not ensembles:
        raise ValueError("No ensemble data provided")
    
    # Find the ensemble with maximum length to determine dimensions
    ensemble_lengths = [(len(ens.times), len(ens.values)) for ens in ensembles]
    max_times = max(times for times, _ in ensemble_lengths)
    max_values = max(values for _, values in ensemble_lengths)
    min_times = min(times for times, _ in ensemble_lengths)
    min_values = min(values for _, values in ensemble_lengths)
    
    logger.info(f"Variable: {variable.name}")
    logger.info(f"Ensemble lengths - min times: {min_times}, max times: {max_times}, "
                f"min values: {min_values}, max values: {max_values}")
    
    # For variables with different temporal resolutions, use the most common length
    # Find the most common time length across ensembles
    time_lengths = [len(ens.times) for ens in ensembles]
    from collections import Counter
    most_common_length = Counter(time_lengths).most_common(1)[0][0]
    
    logger.info(f"Most common time length: {most_common_length}")
    
    # Use ensembles that have the most common length
    valid_ensembles = [ens for ens in ensembles if len(ens.times) == most_common_length]
    
    if not valid_ensembles:
        # Fallback: use minimum length
        num_times = min_times
        valid_ensembles = ensembles
        logger.warning(f"No ensembles with common length found, using all ensembles with min length {num_times}")
    else:
        num_times = most_common_length
        logger.info(f"Using {len(valid_ensembles)} ensembles with length {num_times}")
    
    num_ensembles = len(valid_ensembles)
    
    if num_times == 0:
        raise ValueError("All ensembles are empty")
    
    # Use times from first valid ensemble
    times = valid_ensembles[0].times[:num_times]

    # Collect all values into matrix (time x ensemble)
    all_values = np.zeros((num_times, num_ensembles))

    for ens_idx, ensemble in enumerate(valid_ensembles):
        if len(ensemble.values) != len(ensemble.times):
            logger.warning(f"Ensemble {ensemble.ensemble_id} has mismatched times ({len(ensemble.times)}) "
                         f"and values ({len(ensemble.values)})")

        # Handle deaccumulation if needed
        values = ensemble.values[:num_times]  # Take only available values
        if variable.needs_deaccumulation:
            values = deaccumulate_precipitation(values)

        # Ensure we have enough values and pad with last value if needed
        if len(values) < num_times:
            logger.warning(f"Ensemble {ensemble.ensemble_id} has {len(values)} values, "
                         f"padding to {num_times} with last value")
            if len(values) > 0:
                # Pad with last value
                last_value = values[-1]
                values = values + [last_value] * (num_times - len(values))
            else:
                values = [0.0] * num_times

        all_values[:, ens_idx] = values[:num_times]

    # Check if first timestep is all zeros (analysis time t=0) and skip it
    if num_times > 1 and np.all(all_values[0, :] == 0.0):
        logger.info(f"Skipping first timestep (analysis time t=0) with all zeros for {variable.name}")
        all_values = all_values[1:, :]
        times = times[1:]
        num_times = num_times - 1
    
    # Calculate basic statistics
    mean = np.mean(all_values, axis=1)
    median = np.median(all_values, axis=1)
    std = np.std(all_values, axis=1)
    min_vals = np.min(all_values, axis=1)
    max_vals = np.max(all_values, axis=1)
    
    # Calculate percentiles
    percentiles = {}
    for p in variable.percentiles:
        percentiles[f'{p:02d}'] = np.percentile(all_values, p, axis=1)
    
    return EnsembleStatistics(
        times=times,
        mean=mean.tolist(),
        median=median.tolist(),
        std=std.tolist(),
        min=min_vals.tolist(),
        max=max_vals.tolist(),
        percentiles={k: v.tolist() for k, v in percentiles.items()},
        num_ensembles=num_ensembles
    )


def calculate_derived_precipitation_variables(statistics: EnsembleStatistics,
                                            ensembles: Optional[List[EnsembleMember]] = None
                                           ) -> Dict[str, EnsembleStatistics]:
    """
    Calculate derived precipitation variables (1-hour bins and accumulated).
    
    Args:
        statistics: Base precipitation statistics (rates)
        ensembles: Optional individual ensemble data for more accurate calculations
    
    Returns:
        Dictionary with 'TOT_PREC_1H' and 'TOT_PREC_ACCUM' statistics
    """
    if not statistics.times:
        return {}
    
    # Parse times
    parsed_times = [datetime.fromisoformat(t) for t in statistics.times]
    
    # Calculate time step in minutes
    if len(parsed_times) >= 2:
        time_step_minutes = (parsed_times[1] - parsed_times[0]).total_seconds() / 60
    else:
        time_step_minutes = 5  # Default assumption
    
    logger.info(f"Detected time step: {time_step_minutes} minutes")
    
    derived_vars = {}
    
    # 1-Hour precipitation (binned)
    if time_step_minutes <= 60:  # Only if we have sub-hourly data
        hourly_stats = calculate_hourly_precipitation(statistics, time_step_minutes)
        if hourly_stats:
            derived_vars['TOT_PREC_1H'] = hourly_stats
    
    # Accumulated precipitation
    accumulated_stats = calculate_accumulated_precipitation(statistics, time_step_minutes)
    if accumulated_stats:
        derived_vars['TOT_PREC_ACCUM'] = accumulated_stats
    
    return derived_vars


def calculate_hourly_precipitation(statistics: EnsembleStatistics, 
                                 time_step_minutes: float) -> Optional[EnsembleStatistics]:
    """
    Calculate 1-hour precipitation bins from rate data.
    
    Args:
        statistics: Precipitation rate statistics
        time_step_minutes: Time step in minutes
    
    Returns:
        EnsembleStatistics for 1-hour precipitation
    """
    if time_step_minutes >= 60:
        logger.info("Time step >= 60 minutes, skipping hourly binning")
        return None
    
    steps_per_hour = int(60 / time_step_minutes)
    if steps_per_hour <= 1:
        return None
    
    # Parse times
    parsed_times = [datetime.fromisoformat(t) for t in statistics.times]
    
    # Group into hourly bins
    hourly_data = []
    hourly_times = []
    
    # Process in chunks of steps_per_hour
    for i in range(0, len(statistics.mean), steps_per_hour):
        end_idx = min(i + steps_per_hour, len(statistics.mean))
        
        # Sum rates within the hour (convert rates to accumulation)
        hour_mean = sum(statistics.mean[i:end_idx]) * (time_step_minutes / 60)
        hour_median = sum(statistics.median[i:end_idx]) * (time_step_minutes / 60)
        hour_std = np.sqrt(sum(s**2 for s in statistics.std[i:end_idx])) * (time_step_minutes / 60)
        hour_min = sum(statistics.min[i:end_idx]) * (time_step_minutes / 60)  
        hour_max = sum(statistics.max[i:end_idx]) * (time_step_minutes / 60)
        
        # Calculate percentiles for the hour
        hour_percentiles = {}
        for p_key, p_values in statistics.percentiles.items():
            hour_percentiles[p_key] = sum(p_values[i:end_idx]) * (time_step_minutes / 60)
        
        hourly_data.append({
            'mean': hour_mean,
            'median': hour_median,
            'std': hour_std, 
            'min': hour_min,
            'max': hour_max,
            'percentiles': hour_percentiles
        })
        
        # Use the end time of the hour as the timestamp
        hourly_times.append(parsed_times[end_idx - 1].isoformat())
    
    if not hourly_data:
        return None
    
    # Reconstruct statistics
    return EnsembleStatistics(
        times=hourly_times,
        mean=[h['mean'] for h in hourly_data],
        median=[h['median'] for h in hourly_data],
        std=[h['std'] for h in hourly_data],
        min=[h['min'] for h in hourly_data],
        max=[h['max'] for h in hourly_data],
        percentiles={k: [h['percentiles'][k] for h in hourly_data] 
                    for k in hourly_data[0]['percentiles'].keys()},
        num_ensembles=statistics.num_ensembles
    )


def calculate_accumulated_precipitation(statistics: EnsembleStatistics,
                                      time_step_minutes: float) -> Optional[EnsembleStatistics]:
    """
    Calculate accumulated precipitation from rate data.
    
    Args:
        statistics: Precipitation rate statistics  
        time_step_minutes: Time step in minutes
    
    Returns:
        EnsembleStatistics for accumulated precipitation
    """
    # Convert rates to amounts per time step
    time_step_hours = time_step_minutes / 60
    
    # Calculate cumulative sums
    mean_accum = np.cumsum(np.array(statistics.mean) * time_step_hours).tolist()
    median_accum = np.cumsum(np.array(statistics.median) * time_step_hours).tolist()
    min_accum = np.cumsum(np.array(statistics.min) * time_step_hours).tolist()
    max_accum = np.cumsum(np.array(statistics.max) * time_step_hours).tolist()
    
    # For percentiles
    percentiles_accum = {}
    for p_key, p_values in statistics.percentiles.items():
        percentiles_accum[p_key] = np.cumsum(np.array(p_values) * time_step_hours).tolist()
    
    # Standard deviation for accumulated values is more complex
    # Use simple approximation: accumulate variances
    variances = np.array(statistics.std) ** 2
    std_accum = np.sqrt(np.cumsum(variances * time_step_hours**2)).tolist()
    
    return EnsembleStatistics(
        times=statistics.times,
        mean=mean_accum,
        median=median_accum,
        std=std_accum,
        min=min_accum,
        max=max_accum,
        percentiles=percentiles_accum,
        num_ensembles=statistics.num_ensembles
    )


def validate_ensemble_data(ensembles: List[EnsembleMember]) -> List[str]:
    """
    Validate ensemble data consistency.
    
    Args:
        ensembles: List of ensemble members to validate
    
    Returns:
        List of validation error messages (empty if all valid)
    """
    errors = []
    
    if not ensembles:
        errors.append("No ensemble data provided")
        return errors
    
    # Check time consistency
    reference_times = ensembles[0].times
    reference_length = len(reference_times)
    
    for i, ensemble in enumerate(ensembles):
        if len(ensemble.times) != reference_length:
            errors.append(f"Ensemble {i} has {len(ensemble.times)} times, expected {reference_length}")
        
        if len(ensemble.values) != reference_length:
            errors.append(f"Ensemble {i} has {len(ensemble.values)} values, expected {reference_length}")
        
        # Check for time consistency (first few times)
        check_count = min(3, len(ensemble.times), len(reference_times))
        for j in range(check_count):
            if ensemble.times[j] != reference_times[j]:
                errors.append(f"Ensemble {i} time mismatch at index {j}: {ensemble.times[j]} vs {reference_times[j]}")
                break
    
    # Check for reasonable value ranges
    all_values = []
    for ensemble in ensembles:
        all_values.extend(ensemble.values)
    
    if all_values:
        min_val = min(all_values)
        max_val = max(all_values)
        
        # Basic sanity checks
        if min_val < 0:
            errors.append(f"Found negative values: minimum = {min_val}")
        
        if max_val > 1000:  # Arbitrary large threshold
            errors.append(f"Found very large values: maximum = {max_val}")
    
    return errors


def calculate_ensemble_trends(statistics: EnsembleStatistics) -> Dict[str, float]:
    """
    Calculate trends in ensemble statistics.
    
    Args:
        statistics: Ensemble statistics
    
    Returns:
        Dictionary with trend information
    """
    trends = {}
    
    if len(statistics.mean) < 2:
        return trends
    
    # Linear trend in mean values
    x = np.arange(len(statistics.mean))
    mean_trend = np.polyfit(x, statistics.mean, 1)[0]  # Slope
    
    # Trend in variability (std)
    std_trend = np.polyfit(x, statistics.std, 1)[0]
    
    # Peak information
    mean_peak_idx = np.argmax(statistics.mean)
    mean_peak_time = statistics.times[mean_peak_idx]
    mean_peak_value = statistics.mean[mean_peak_idx]
    
    trends.update({
        'mean_trend': float(mean_trend),
        'std_trend': float(std_trend),
        'peak_time': mean_peak_time,
        'peak_value': float(mean_peak_value),
        'total_mean': float(np.sum(statistics.mean)),
        'max_std': float(np.max(statistics.std))
    })
    
    return trends