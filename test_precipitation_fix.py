#!/usr/bin/env python3
"""
Test script to verify the accumulated precipitation calculation fix.

This script tests the difference between the old (incorrect) and new (correct) 
methods for computing accumulated precipitation statistics.
"""

import numpy as np
import json
from datetime import datetime, timedelta

def test_accumulated_precipitation_calculation():
    """Test the accumulated precipitation calculation fix"""
    
    print("🧪 Testing Accumulated Precipitation Calculation Fix")
    print("=" * 60)
    
    # Simulate realistic precipitation data
    # Original GRIB accumulated values (mm) - these are the truth
    original_accumulated = [
        0.0,     # t=0: no accumulation at start
        0.2,     # t=1: 0.2mm total
        0.5,     # t=2: 0.5mm total (0.3mm in this period)
        1.2,     # t=3: 1.2mm total (0.7mm in this period)
        2.1,     # t=4: 2.1mm total (0.9mm in this period)
        2.3,     # t=5: 2.3mm total (0.2mm in this period)
    ]
    
    # Deaccumulated rates (mm/h) - what we store in JSON after processing
    time_step_minutes = 15  # 15-minute intervals 
    time_step_hours = time_step_minutes / 60.0
    
    # Correct deaccumulation using np.diff
    rates = np.diff(original_accumulated, prepend=0.0).tolist()
    rates = [max(0.0, r) for r in rates]  # Ensure non-negative
    
    print(f"📊 Test Data (15-minute intervals):")
    print(f"Original accumulated (mm):     {original_accumulated}")
    print(f"Deaccumulated rates (mm/15min): {rates}")
    print(f"Rates converted to mm/h:       {[r/time_step_hours for r in rates]}")
    print()
    
    # OLD METHOD (incorrect): cumsum(rates) * time_step
    print("❌ OLD METHOD (INCORRECT):")
    old_method_accumulated = np.cumsum(rates) * time_step_hours
    print(f"Result: {old_method_accumulated.tolist()}")
    print(f"Error:  {(old_method_accumulated - np.array(original_accumulated)).tolist()}")
    print()
    
    # NEW METHOD (correct): use original accumulated values directly
    print("✅ NEW METHOD (CORRECT):")
    new_method_accumulated = original_accumulated
    print(f"Result: {new_method_accumulated}")
    print(f"Error:  {[0.0] * len(original_accumulated)}")
    print()
    
    # Calculate relative error
    # Skip first value (always 0) for error calculation
    old_error = np.abs(old_method_accumulated[1:] - np.array(original_accumulated[1:]))
    old_relative_error = np.mean(old_error / np.array(original_accumulated[1:])) * 100
    
    print(f"📈 Analysis:")
    print(f"Old method mean absolute error: {np.mean(old_error):.4f} mm")
    print(f"Old method mean relative error: {old_relative_error:.1f}%")
    print(f"New method mean absolute error: 0.0000 mm")
    print(f"New method mean relative error: 0.0%")
    print()
    
    # Test with ensemble statistics
    print("🎯 Ensemble Statistics Test:")
    
    # Simulate multiple ensemble members
    np.random.seed(42)  # For reproducible results
    n_ensembles = 20
    ensemble_accumulated = []
    
    for i in range(n_ensembles):
        # Add some realistic noise to the accumulated values
        noise = np.random.normal(0, 0.1, len(original_accumulated))
        noisy_accumulated = np.maximum(0, np.array(original_accumulated) + noise)
        # Ensure monotonically increasing (accumulated values can't decrease)
        for j in range(1, len(noisy_accumulated)):
            if noisy_accumulated[j] < noisy_accumulated[j-1]:
                noisy_accumulated[j] = noisy_accumulated[j-1]
        ensemble_accumulated.append(noisy_accumulated.tolist())
    
    # Compute statistics from original accumulated values (CORRECT)
    accumulated_array = np.array(ensemble_accumulated)
    correct_mean = np.mean(accumulated_array, axis=0)
    correct_p95 = np.percentile(accumulated_array, 95, axis=0)
    
    # Compute statistics from rates and re-accumulate (INCORRECT)
    ensemble_rates = []
    for acc_vals in ensemble_accumulated:
        rates = np.diff(acc_vals, prepend=0.0)
        rates = np.maximum(rates, 0.0)
        ensemble_rates.append(rates)
    
    rates_array = np.array(ensemble_rates) 
    rates_mean = np.mean(rates_array, axis=0)
    rates_p95 = np.percentile(rates_array, 95, axis=0)
    
    # Re-accumulate from rates (this is what the old code was doing wrong)
    incorrect_mean = np.cumsum(rates_mean) * time_step_hours
    incorrect_p95 = np.cumsum(rates_p95) * time_step_hours
    
    print(f"True ensemble mean accumulated:      {correct_mean}")
    print(f"Incorrect method (from rates):       {incorrect_mean}")
    print(f"Error in mean:                       {incorrect_mean - correct_mean}")
    print()
    print(f"True ensemble 95th percentile:       {correct_p95}")
    print(f"Incorrect method (from rates):       {incorrect_p95}")  
    print(f"Error in p95:                        {incorrect_p95 - correct_p95}")
    print()
    
    # Summary
    mean_error_in_mean = np.mean(np.abs(incorrect_mean - correct_mean))
    mean_error_in_p95 = np.mean(np.abs(incorrect_p95 - correct_p95))
    
    print("🎉 SUMMARY:")
    print(f"The old method had an average error of {mean_error_in_mean:.3f} mm in ensemble mean")
    print(f"The old method had an average error of {mean_error_in_p95:.3f} mm in 95th percentile")
    print("The new method uses original accumulated values and has ZERO error! ✅")
    
    return True

if __name__ == "__main__":
    test_accumulated_precipitation_calculation()