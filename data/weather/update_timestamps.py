#!/usr/bin/env python3
"""
Script to update timestamps in forecast JSON file from ISO format to new format.
Converts timestamps from "2025-08-31T06:05:00" to "2025-08-31T_0605"
"""

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List


def convert_timestamp(timestamp_str: str) -> str:
    """
    Convert timestamp from ISO format to new format.
    
    Args:
        timestamp_str: Timestamp in format "2025-08-31T06:05:00"
    
    Returns:
        Timestamp in format "2025-08-31T_0605"
    """
    # Parse the ISO format timestamp
    dt = datetime.fromisoformat(timestamp_str)
    
    # Convert to new format: %Y-%m-%dT_%H%M
    return dt.strftime("%Y-%m-%dT_%H%M")


def update_timestamps_in_data(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively update all timestamp strings in the data structure.
    
    Args:
        data: The JSON data structure
        
    Returns:
        Updated data structure with new timestamp format
    """
    if isinstance(data, dict):
        updated_data = {}
        for key, value in data.items():
            if key == "times" and isinstance(value, list):
                # Convert all timestamps in the times array
                updated_data[key] = [convert_timestamp(ts) for ts in value]
            else:
                updated_data[key] = update_timestamps_in_data(value)
        return updated_data
    elif isinstance(data, list):
        return [update_timestamps_in_data(item) for item in data]
    else:
        return data


def main():
    """Main function to update timestamps in the forecast JSON file."""
    
    # Path to the forecast file
    file_path = Path("forecast_2025-08-31T06%3A00.json")
    
    if not file_path.exists():
        print(f"Error: File {file_path} not found!")
        return
    
    print(f"Loading forecast data from {file_path}")
    
    # Load the JSON data
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading JSON file: {e}")
        return
    
    # Show some examples of current timestamps
    print("\n=== BEFORE CONVERSION ===")
    if 'variables' in data:
        for var_name, var_data in data['variables'].items():
            if 'times' in var_data and var_data['times']:
                print(f"{var_name} timestamps (first 5):")
                for i, ts in enumerate(var_data['times'][:5]):
                    print(f"  {i+1}: {ts}")
                break
    
    print(f"\nTotal timestamps to convert: ", end="")
    total_timestamps = 0
    if 'variables' in data:
        for var_data in data['variables'].values():
            if 'times' in var_data:
                total_timestamps += len(var_data['times'])
    print(total_timestamps)
    
    # Update timestamps
    print(f"\nConverting timestamps to new format...")
    updated_data = update_timestamps_in_data(data)
    
    # Show some examples of converted timestamps
    print("\n=== AFTER CONVERSION ===")
    if 'variables' in updated_data:
        for var_name, var_data in updated_data['variables'].items():
            if 'times' in var_data and var_data['times']:
                print(f"{var_name} timestamps (first 5):")
                for i, ts in enumerate(var_data['times'][:5]):
                    print(f"  {i+1}: {ts}")
                break
    
    # Show before/after comparison
    print("\n=== COMPARISON EXAMPLES ===")
    if 'variables' in data and 'variables' in updated_data:
        original_times = None
        updated_times = None
        
        # Get the first variable's times
        for var_data in data['variables'].values():
            if 'times' in var_data:
                original_times = var_data['times']
                break
                
        for var_data in updated_data['variables'].values():
            if 'times' in var_data:
                updated_times = var_data['times']
                break
        
        if original_times and updated_times:
            print("Before -> After:")
            for i in range(min(10, len(original_times))):
                print(f"  {original_times[i]} -> {updated_times[i]}")
    
    # Save the updated data
    backup_path = file_path.with_suffix('.json.backup')
    print(f"\nCreating backup at {backup_path}")
    
    try:
        # Create backup
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
        
        # Save updated data
        print(f"Saving updated data to {file_path}")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(updated_data, f, indent=2)
            
        print("✅ Successfully updated timestamps!")
        print(f"   Original file backed up to: {backup_path}")
        print(f"   Updated file saved to: {file_path}")
        
    except Exception as e:
        print(f"❌ Error saving files: {e}")
        return
    
    # Verification
    print(f"\n=== VERIFICATION ===")
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            verification_data = json.load(f)
        
        timestamp_count = 0
        if 'variables' in verification_data:
            for var_data in verification_data['variables'].values():
                if 'times' in var_data:
                    timestamp_count += len(var_data['times'])
        
        print(f"✅ File successfully loaded after update")
        print(f"✅ {timestamp_count} timestamps verified in updated file")
        
        # Check format of first timestamp
        if 'variables' in verification_data:
            for var_data in verification_data['variables'].values():
                if 'times' in var_data and var_data['times']:
                    first_ts = var_data['times'][0]
                    if re.match(r'\d{4}-\d{2}-\d{2}T_\d{4}', first_ts):
                        print(f"✅ New timestamp format confirmed: {first_ts}")
                    else:
                        print(f"⚠️  Unexpected timestamp format: {first_ts}")
                    break
        
    except Exception as e:
        print(f"❌ Error verifying updated file: {e}")


if __name__ == "__main__":
    main()