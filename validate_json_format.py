#!/usr/bin/env python3
"""
Weather JSON Format Validation Script
=====================================

Validates all weather forecast JSON files in the data directory to ensure they
match the expected schema and identifies any files with format issues.

This script can:
1. Scan all JSON files in the weather data directory
2. Validate each file against the Pydantic schema
3. Report format issues and inconsistencies
4. Fix files that have the old incorrect format
5. Generate summary reports

Usage:
    python validate_json_format.py                    # Validate all files
    python validate_json_format.py --fix              # Fix incorrect formats
    python validate_json_format.py --summary          # Show summary only
    python validate_json_format.py --file <path>      # Validate specific file
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Dict, List, Any
from datetime import datetime

try:
    from weather_models import WeatherForecastValidator, WeatherForecast
    VALIDATION_AVAILABLE = True
except ImportError:
    print("❌ Weather validation models not available. Please install dependencies.")
    sys.exit(1)


class ValidationReport:
    """Manages validation results and reporting"""
    
    def __init__(self):
        self.results: List[Dict[str, Any]] = []
        self.total_files = 0
        self.valid_files = 0
        self.invalid_files = 0
        self.old_format_files = 0
        self.missing_files = 0
    
    def add_result(self, result: Dict[str, Any]):
        """Add a validation result"""
        self.results.append(result)
        self.total_files += 1
        
        if not result['exists']:
            self.missing_files += 1
        elif result['valid']:
            self.valid_files += 1
        else:
            self.invalid_files += 1
            
        if result['is_old_format']:
            self.old_format_files += 1
    
    def print_summary(self):
        """Print validation summary"""
        print("\n📊 Validation Summary")
        print("=" * 50)
        print(f"📁 Total files scanned:     {self.total_files}")
        print(f"✅ Valid files:             {self.valid_files}")
        print(f"❌ Invalid files:           {self.invalid_files}")
        print(f"📄 Old format files:        {self.old_format_files}")
        print(f"❓ Missing files:           {self.missing_files}")
        
        if self.valid_files > 0:
            print(f"🎯 Success rate:            {self.valid_files/max(self.total_files-self.missing_files,1)*100:.1f}%")
    
    def print_detailed_results(self, show_valid: bool = False):
        """Print detailed validation results"""
        print("\n📋 Detailed Results")
        print("=" * 50)
        
        for result in self.results:
            status_icon = "❓" if not result['exists'] else ("✅" if result['valid'] else "❌")
            size_info = f"{result['size_kb']:.1f}KB" if result['exists'] else "N/A"
            
            print(f"{status_icon} {Path(result['file']).name:<35} {size_info:>8}")
            
            if result['is_old_format']:
                print(f"   ⚠️ Old format detected")
            
            if result['errors'] and (not result['valid'] or show_valid):
                for error in result['errors'][:3]:  # Show first 3 errors
                    print(f"   • {error}")
                if len(result['errors']) > 3:
                    print(f"   • ... and {len(result['errors']) - 3} more errors")
    
    def get_files_to_fix(self) -> List[str]:
        """Get list of files that need fixing"""
        return [r['file'] for r in self.results if r['is_old_format'] or not r['valid']]


def find_weather_json_files(data_dir: str = "data/weather") -> List[str]:
    """Find all weather JSON files in the data directory"""
    data_path = Path(data_dir)
    if not data_path.exists():
        print(f"❌ Data directory not found: {data_dir}")
        return []
    
    # Find all JSON files that look like forecast files
    json_files = []
    for json_file in data_path.glob("**/*.json"):
        if json_file.name.startswith("forecast_") or "forecast" in json_file.name:
            json_files.append(str(json_file))
    
    return sorted(json_files)


def validate_file(file_path: str, verbose: bool = False) -> Dict[str, Any]:
    """Validate a single JSON file"""
    if verbose:
        print(f"🔍 Validating {Path(file_path).name}...")
    
    result = WeatherForecastValidator.detect_format_issues(file_path)
    
    if verbose and result['exists']:
        if result['valid']:
            print(f"   ✅ Valid - {result['size_kb']:.1f}KB")
        else:
            print(f"   ❌ Invalid - {len(result['errors'])} errors")
            for error in result['errors'][:2]:
                print(f"      • {error}")
        
        if result['is_old_format']:
            print(f"   ⚠️ Old format detected")
    
    return result


def fix_old_format_file(file_path: str, backup: bool = True) -> bool:
    """
    Attempt to fix a file with old format by converting it to new format
    
    Note: This is a placeholder - actual format conversion would depend on 
    the specific structure of the old format and available data sources.
    """
    print(f"🔧 Attempting to fix {Path(file_path).name}...")
    
    try:
        # Create backup if requested
        if backup:
            backup_path = str(file_path) + ".backup"
            Path(file_path).rename(backup_path)
            print(f"   💾 Backup created: {Path(backup_path).name}")
        
        # For now, we can't automatically convert old format to new format
        # This would require re-processing the original data
        print(f"   ⚠️ Automatic format conversion not implemented")
        print(f"   💡 Suggestion: Re-run the pipeline to regenerate this file")
        
        return False
        
    except Exception as e:
        print(f"   ❌ Failed to fix file: {e}")
        return False


def validate_all_files(data_dir: str, verbose: bool = False) -> ValidationReport:
    """Validate all weather JSON files in the directory"""
    print(f"🔍 Scanning for weather JSON files in {data_dir}...")
    
    json_files = find_weather_json_files(data_dir)
    if not json_files:
        print(f"❌ No weather JSON files found in {data_dir}")
        return ValidationReport()
    
    print(f"📁 Found {len(json_files)} weather JSON files")
    
    report = ValidationReport()
    
    for file_path in json_files:
        result = validate_file(file_path, verbose)
        report.add_result(result)
    
    return report


def main():
    parser = argparse.ArgumentParser(
        description='Validate weather forecast JSON files against expected schema',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Examples:
  python validate_json_format.py                    # Validate all files
  python validate_json_format.py --verbose          # Show detailed progress
  python validate_json_format.py --summary          # Show summary only
  python validate_json_format.py --fix              # Attempt to fix issues
  python validate_json_format.py --file data/weather/forecast_2025-08-30T22%3A00.json
        '''
    )
    
    parser.add_argument('--data-dir', default='data/weather',
                       help='Directory to scan for JSON files (default: data/weather)')
    parser.add_argument('--file', 
                       help='Validate specific file instead of scanning directory')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='Show detailed validation progress')
    parser.add_argument('--summary', action='store_true',
                       help='Show summary only (no detailed results)')
    parser.add_argument('--fix', action='store_true',
                       help='Attempt to fix files with format issues')
    parser.add_argument('--show-valid', action='store_true',
                       help='Show details for valid files too')
    
    args = parser.parse_args()
    
    print("🔬 Weather JSON Format Validator")
    print("=" * 40)
    
    if args.file:
        # Validate single file
        if not Path(args.file).exists():
            print(f"❌ File not found: {args.file}")
            sys.exit(1)
        
        result = validate_file(args.file, verbose=True)
        
        if result['valid']:
            print(f"\n✅ File is valid!")
        else:
            print(f"\n❌ File has validation errors:")
            for error in result['errors']:
                print(f"   • {error}")
        
        if result['is_old_format']:
            print(f"\n⚠️ File uses old format structure")
            
        sys.exit(0 if result['valid'] else 1)
    
    # Validate all files in directory
    report = validate_all_files(args.data_dir, args.verbose)
    
    report.print_summary()
    
    if not args.summary:
        report.print_detailed_results(args.show_valid)
    
    # Fix files if requested
    if args.fix:
        files_to_fix = report.get_files_to_fix()
        if files_to_fix:
            print(f"\n🔧 Attempting to fix {len(files_to_fix)} files...")
            fixed_count = 0
            for file_path in files_to_fix:
                if fix_old_format_file(file_path):
                    fixed_count += 1
            
            print(f"✅ Fixed {fixed_count}/{len(files_to_fix)} files")
        else:
            print(f"\n✅ No files need fixing")
    
    # Exit with appropriate code
    if report.invalid_files > 0 or report.old_format_files > 0:
        print(f"\n⚠️ Found {report.invalid_files + report.old_format_files} files with issues")
        if not args.fix:
            print(f"💡 Run with --fix to attempt automatic fixes")
        sys.exit(1)
    else:
        print(f"\n✅ All files are valid!")
        sys.exit(0)


if __name__ == "__main__":
    main()