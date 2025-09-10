#!/usr/bin/env python3
"""
Test Script for Weather Data Schema Fixes
=========================================

This script validates and applies the schema fixes to weather forecast data:
1. Remove variable prefixes from ensemble_statistics (use "mean", "median" instead of "tp_mean", "vmax_mean")
2. Change VMAX_10M name from "Maximum Wind Speed at 10m" to "Wind Gust"
"""

import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any


def load_forecast_data(filepath: Path) -> Dict[str, Any]:
    """Load existing forecast JSON data"""
    print(f"Loading forecast data from: {filepath}")
    with open(filepath, 'r') as f:
        return json.load(f)


def fix_ensemble_statistics(statistics: Dict[str, Any]) -> Dict[str, Any]:
    """Fix ensemble statistics by removing variable prefixes"""
    fixed_stats = {}
    
    # Mapping from prefixed names to clean names
    field_mappings = {
        'tp_mean': 'mean',
        'tp_median': 'median', 
        'tp_std': 'std',
        'tp_min': 'min',
        'tp_max': 'max',
        'vmax_mean': 'mean',
        'vmax_median': 'median',
        'vmax_std': 'std', 
        'vmax_min': 'min',
        'vmax_max': 'max'
    }
    
    for old_key, values in statistics.items():
        # Map prefixed names to clean names
        if old_key in field_mappings:
            new_key = field_mappings[old_key]
            fixed_stats[new_key] = values
        # Keep percentile keys as they are (tp_05 -> p05, tp_95 -> p95, etc.)
        elif old_key.startswith(('tp_', 'vmax_')):
            # Extract percentile number and create clean percentile key
            percentile = old_key.split('_')[1]  # Extract "05", "95", etc.
            if percentile.isdigit():
                new_key = f'p{percentile}'
                fixed_stats[new_key] = values
            else:
                # Keep other fields as-is
                fixed_stats[old_key] = values
        else:
            # Keep non-prefixed fields as-is
            fixed_stats[old_key] = values
    
    return fixed_stats


def fix_variable_names(variables: Dict[str, Any]) -> Dict[str, Any]:
    """Fix variable names according to schema updates"""
    fixed_vars = {}
    
    for var_id, var_data in variables.items():
        fixed_var = var_data.copy()
        
        # Fix VMAX_10M name
        if var_id == 'VMAX_10M' and var_data.get('name') == 'Maximum Wind Speed at 10m':
            print(f"  Fixing VMAX_10M name: '{var_data['name']}' -> 'Wind Gust'")
            fixed_var['name'] = 'Wind Gust'
        
        # Fix ensemble_statistics if present
        if 'ensemble_statistics' in var_data:
            print(f"  Fixing ensemble_statistics for {var_id}")
            original_stats = var_data['ensemble_statistics']
            fixed_stats = fix_ensemble_statistics(original_stats)
            fixed_var['ensemble_statistics'] = fixed_stats
        
        fixed_vars[var_id] = fixed_var
    
    return fixed_vars


def apply_schema_fixes(data: Dict[str, Any]) -> Dict[str, Any]:
    """Apply all schema fixes to forecast data"""
    print("Applying schema fixes...")
    
    fixed_data = data.copy()
    
    # Fix variables if present
    if 'variables' in data:
        fixed_data['variables'] = fix_variable_names(data['variables'])
    
    # Update processed_at timestamp
    fixed_data['processed_at'] = datetime.utcnow().isoformat()
    
    return fixed_data


def show_before_after_comparison(original: Dict[str, Any], fixed: Dict[str, Any]):
    """Show key differences between original and fixed data"""
    print("\n" + "="*60)
    print("BEFORE/AFTER COMPARISON")
    print("="*60)
    
    # Compare variable names
    print("\n1. Variable Names:")
    if 'variables' in original and 'variables' in fixed:
        for var_id in original['variables']:
            orig_name = original['variables'][var_id].get('name', 'N/A')
            fixed_name = fixed['variables'][var_id].get('name', 'N/A')
            
            if orig_name != fixed_name:
                print(f"   {var_id}:")
                print(f"     BEFORE: '{orig_name}'")
                print(f"     AFTER:  '{fixed_name}'")
            else:
                print(f"   {var_id}: '{orig_name}' (unchanged)")
    
    # Compare ensemble statistics structure 
    print("\n2. Ensemble Statistics Structure:")
    if 'variables' in original:
        for var_id in original['variables']:
            orig_stats = original['variables'][var_id].get('ensemble_statistics', {})
            fixed_stats = fixed['variables'][var_id].get('ensemble_statistics', {})
            
            if orig_stats != fixed_stats:
                print(f"\n   {var_id}:")
                
                # Show key changes
                orig_keys = set(orig_stats.keys())
                fixed_keys = set(fixed_stats.keys())
                
                removed_keys = orig_keys - fixed_keys
                added_keys = fixed_keys - orig_keys
                
                if removed_keys:
                    print(f"     REMOVED: {sorted(removed_keys)}")
                if added_keys:
                    print(f"     ADDED:   {sorted(added_keys)}")
                
                # Show specific mappings for main statistics
                main_stats = ['mean', 'median', 'std', 'min', 'max']
                for stat in main_stats:
                    prefix_key = f'tp_{stat}' if var_id == 'TOT_PREC' else f'vmax_{stat}'
                    if prefix_key in orig_stats and stat in fixed_stats:
                        # Quick comparison - just check if arrays have same length
                        orig_len = len(orig_stats[prefix_key]) if isinstance(orig_stats[prefix_key], list) else 1
                        fixed_len = len(fixed_stats[stat]) if isinstance(fixed_stats[stat], list) else 1
                        status = "✓" if orig_len == fixed_len else "✗"
                        print(f"     {prefix_key} -> {stat}: {status} (len: {orig_len} -> {fixed_len})")
    
    print("\n3. Processing Timestamp:")
    print(f"   BEFORE: {original.get('processed_at', 'N/A')}")
    print(f"   AFTER:  {fixed.get('processed_at', 'N/A')}")
    print()


def main():
    """Main test function"""
    print("Weather Data Schema Fixes Test")
    print("=" * 40)
    
    # File paths
    data_dir = Path(__file__).parent / "data" / "weather"
    forecast_file = data_dir / "forecast_2025-08-31T06%3A00.json"
    backup_file = forecast_file.with_suffix('.json.backup')
    
    if not forecast_file.exists():
        print(f"ERROR: Forecast file not found: {forecast_file}")
        return 1
    
    try:
        # 1. Load original data
        original_data = load_forecast_data(forecast_file)
        print(f"✓ Loaded {len(original_data)} top-level fields")
        
        # 2. Create backup
        print(f"Creating backup: {backup_file}")
        shutil.copy2(forecast_file, backup_file)
        
        # 3. Apply fixes
        fixed_data = apply_schema_fixes(original_data)
        print("✓ Applied schema fixes")
        
        # 4. Show comparison
        show_before_after_comparison(original_data, fixed_data)
        
        # 5. Save fixed data
        print(f"Saving fixed data to: {forecast_file}")
        with open(forecast_file, 'w') as f:
            json.dump(fixed_data, f, indent=2)
        print("✓ Saved corrected JSON file")
        
        # 6. Validation
        print("\nValidating fixed data...")
        
        # Check VMAX_10M name
        vmax_data = fixed_data.get('variables', {}).get('VMAX_10M', {})
        if vmax_data.get('name') == 'Wind Gust':
            print("✓ VMAX_10M name correctly set to 'Wind Gust'")
        else:
            print(f"✗ VMAX_10M name incorrect: '{vmax_data.get('name')}'")
        
        # Check ensemble statistics have no prefixes
        for var_id in ['TOT_PREC', 'VMAX_10M']:
            stats = fixed_data.get('variables', {}).get(var_id, {}).get('ensemble_statistics', {})
            prefixed_keys = [k for k in stats.keys() if k.startswith(('tp_', 'vmax_')) and not k.startswith(('tp_0', 'tp_1', 'tp_2', 'tp_3', 'tp_4', 'tp_5', 'tp_6', 'tp_7', 'tp_8', 'tp_9'))]
            
            if prefixed_keys:
                print(f"✗ {var_id} still has prefixed keys: {prefixed_keys}")
            else:
                main_keys = [k for k in stats.keys() if k in ['mean', 'median', 'std', 'min', 'max']]
                print(f"✓ {var_id} ensemble statistics clean: {main_keys}")
        
        print(f"\nSchema fixes applied successfully!")
        print(f"Backup saved as: {backup_file}")
        return 0
        
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())