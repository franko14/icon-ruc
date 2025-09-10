#!/usr/bin/env python3
"""
Weather Data Cleanup Module
===========================

Provides specialized cleanup functionality for weather forecast data with 12-hour retention.
Cleans both raw GRIB files and processed JSON forecast files.

Usage:
  from weather_cleanup import WeatherCleanup
  cleanup = WeatherCleanup()
  cleanup.cleanup_old_data()
"""

import os
import time
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

# Configure logging
logger = logging.getLogger(__name__)


class WeatherCleanup:
    """Manages cleanup of weather forecast data with 12-hour retention policy."""
    
    def __init__(self, retention_hours: int = 12):
        """
        Initialize weather cleanup manager.
        
        Args:
            retention_hours: Hours to keep data (default: 12)
        """
        self.retention_hours = retention_hours
        self.data_dir = Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.weather_dir = self.data_dir / "weather" 
        self.bratislava_dir = self.data_dir / "bratislava"
        
        # Ensure directories exist
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.weather_dir.mkdir(parents=True, exist_ok=True)
    
    def log(self, message: str, level: str = "INFO"):
        """Log message with timestamp."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[WEATHER_CLEANUP {timestamp}] {message}")
        
        if level == "ERROR":
            logger.error(message)
        elif level == "WARNING":
            logger.warning(message)
        else:
            logger.info(message)
    
    def get_cutoff_time(self) -> datetime:
        """Get the cutoff time for data retention."""
        return datetime.now() - timedelta(hours=self.retention_hours)
    
    def get_file_age_hours(self, file_path: Path) -> float:
        """Get the age of a file in hours."""
        try:
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            age = datetime.now() - file_mtime
            return age.total_seconds() / 3600
        except Exception as e:
            self.log(f"Error getting age for {file_path}: {e}", "ERROR")
            return 0
    
    def cleanup_raw_data(self) -> Dict:
        """Clean up raw GRIB files older than retention period."""
        self.log("🧹 Cleaning up raw GRIB files...")
        
        stats = {
            'files_deleted': 0,
            'space_freed_bytes': 0,
            'errors': []
        }
        
        if not self.raw_dir.exists():
            self.log("📁 Raw data directory doesn't exist, skipping raw cleanup")
            return stats
        
        cutoff_time = self.get_cutoff_time()
        
        try:
            # Find all GRIB files
            grib_patterns = ['*.grib2', '*.grb', '*.grib']
            grib_files = []
            
            for pattern in grib_patterns:
                grib_files.extend(self.raw_dir.rglob(pattern))
            
            old_files = []
            for grib_file in grib_files:
                try:
                    file_mtime = datetime.fromtimestamp(grib_file.stat().st_mtime)
                    if file_mtime < cutoff_time:
                        old_files.append((grib_file, file_mtime))
                except Exception as e:
                    stats['errors'].append(f"Error checking {grib_file}: {e}")
            
            # Delete old files
            for file_path, file_time in old_files:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    
                    stats['files_deleted'] += 1
                    stats['space_freed_bytes'] += file_size
                    
                    age_hours = self.get_file_age_hours(file_path) if file_path.exists() else (datetime.now() - file_time).total_seconds() / 3600
                    self.log(f"🗑️  Deleted raw file: {file_path.name} (age: {age_hours:.1f}h, size: {file_size/1024/1024:.1f} MB)")
                
                except Exception as e:
                    stats['errors'].append(f"Error deleting {file_path}: {e}")
            
            if stats['files_deleted'] == 0:
                self.log("✅ No old raw files found to delete")
            else:
                space_gb = stats['space_freed_bytes'] / (1024**3)
                self.log(f"✅ Deleted {stats['files_deleted']} raw files, freed {space_gb:.2f} GB")
        
        except Exception as e:
            self.log(f"❌ Error during raw cleanup: {e}", "ERROR")
            stats['errors'].append(f"Raw cleanup error: {e}")
        
        return stats
    
    def cleanup_forecast_jsons(self) -> Dict:
        """Clean up processed forecast JSON files older than retention period."""
        self.log("🧹 Cleaning up forecast JSON files...")
        
        stats = {
            'files_deleted': 0,
            'directories_deleted': 0,
            'space_freed_bytes': 0,
            'errors': []
        }
        
        if not self.weather_dir.exists():
            self.log("📁 Weather data directory doesn't exist, skipping forecast cleanup")
            return stats
        
        cutoff_time = self.get_cutoff_time()
        
        try:
            # Find forecast JSON files
            forecast_files = list(self.weather_dir.glob("forecast_*.json"))
            ensemble_dirs = [d for d in self.weather_dir.iterdir() if d.is_dir() and d.name.startswith("forecast_")]
            
            old_files = []
            old_dirs = []
            
            # Check individual JSON files
            for json_file in forecast_files:
                try:
                    file_mtime = datetime.fromtimestamp(json_file.stat().st_mtime)
                    if file_mtime < cutoff_time:
                        old_files.append((json_file, file_mtime))
                except Exception as e:
                    stats['errors'].append(f"Error checking {json_file}: {e}")
            
            # Check ensemble directories
            for ensemble_dir in ensemble_dirs:
                try:
                    dir_mtime = datetime.fromtimestamp(ensemble_dir.stat().st_mtime)
                    if dir_mtime < cutoff_time:
                        old_dirs.append((ensemble_dir, dir_mtime))
                except Exception as e:
                    stats['errors'].append(f"Error checking {ensemble_dir}: {e}")
            
            # Delete old JSON files
            for file_path, file_time in old_files:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    
                    stats['files_deleted'] += 1
                    stats['space_freed_bytes'] += file_size
                    
                    age_hours = (datetime.now() - file_time).total_seconds() / 3600
                    self.log(f"🗑️  Deleted forecast file: {file_path.name} (age: {age_hours:.1f}h, size: {file_size/1024:.1f} KB)")
                
                except Exception as e:
                    stats['errors'].append(f"Error deleting {file_path}: {e}")
            
            # Delete old ensemble directories
            for dir_path, dir_time in old_dirs:
                try:
                    # Calculate directory size
                    dir_size = sum(f.stat().st_size for f in dir_path.rglob('*') if f.is_file())
                    
                    # Count files in directory
                    file_count = len([f for f in dir_path.rglob('*') if f.is_file()])
                    
                    # Remove directory and all contents
                    import shutil
                    shutil.rmtree(dir_path)
                    
                    stats['directories_deleted'] += 1
                    stats['files_deleted'] += file_count
                    stats['space_freed_bytes'] += dir_size
                    
                    age_hours = (datetime.now() - dir_time).total_seconds() / 3600
                    self.log(f"🗂️  Deleted ensemble directory: {dir_path.name} (age: {age_hours:.1f}h, files: {file_count}, size: {dir_size/1024:.1f} KB)")
                
                except Exception as e:
                    stats['errors'].append(f"Error deleting directory {dir_path}: {e}")
            
            if stats['files_deleted'] == 0 and stats['directories_deleted'] == 0:
                self.log("✅ No old forecast files found to delete")
            else:
                space_mb = stats['space_freed_bytes'] / (1024**2)
                self.log(f"✅ Deleted {stats['files_deleted']} files and {stats['directories_deleted']} directories, freed {space_mb:.1f} MB")
        
        except Exception as e:
            self.log(f"❌ Error during forecast cleanup: {e}", "ERROR")
            stats['errors'].append(f"Forecast cleanup error: {e}")
        
        return stats
    
    def cleanup_bratislava_data(self) -> Dict:
        """Clean up old Bratislava-specific data if it exists."""
        self.log("🧹 Cleaning up Bratislava data...")
        
        stats = {
            'files_deleted': 0,
            'space_freed_bytes': 0,
            'errors': []
        }
        
        if not self.bratislava_dir.exists():
            self.log("📁 Bratislava data directory doesn't exist, skipping")
            return stats
        
        cutoff_time = self.get_cutoff_time()
        
        try:
            # Find NetCDF files in bratislava directory
            nc_files = list(self.bratislava_dir.rglob("*.nc"))
            json_files = list(self.bratislava_dir.rglob("*.json"))
            
            all_files = nc_files + json_files
            old_files = []
            
            for file_path in all_files:
                try:
                    file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                    if file_mtime < cutoff_time:
                        old_files.append((file_path, file_mtime))
                except Exception as e:
                    stats['errors'].append(f"Error checking {file_path}: {e}")
            
            # Delete old files
            for file_path, file_time in old_files:
                try:
                    file_size = file_path.stat().st_size
                    file_path.unlink()
                    
                    stats['files_deleted'] += 1
                    stats['space_freed_bytes'] += file_size
                    
                    age_hours = (datetime.now() - file_time).total_seconds() / 3600
                    self.log(f"🗑️  Deleted Bratislava file: {file_path.name} (age: {age_hours:.1f}h, size: {file_size/1024:.1f} KB)")
                
                except Exception as e:
                    stats['errors'].append(f"Error deleting {file_path}: {e}")
            
            if stats['files_deleted'] == 0:
                self.log("✅ No old Bratislava files found to delete")
            else:
                space_mb = stats['space_freed_bytes'] / (1024**2)
                self.log(f"✅ Deleted {stats['files_deleted']} Bratislava files, freed {space_mb:.1f} MB")
        
        except Exception as e:
            self.log(f"❌ Error during Bratislava cleanup: {e}", "ERROR")
            stats['errors'].append(f"Bratislava cleanup error: {e}")
        
        return stats
    
    def cleanup_old_data(self) -> Dict:
        """
        Perform complete cleanup of all old weather data.
        
        Returns:
            Dictionary with cleanup statistics
        """
        self.log(f"🚀 Starting weather data cleanup (retention: {self.retention_hours} hours)")
        start_time = time.time()
        
        # Combine all cleanup operations
        total_stats = {
            'cleanup_performed': True,
            'retention_hours': self.retention_hours,
            'total_files_deleted': 0,
            'total_directories_deleted': 0,
            'total_space_freed_bytes': 0,
            'operations': {},
            'errors': [],
            'duration_seconds': 0
        }
        
        # Cleanup operations
        operations = [
            ('raw_data', self.cleanup_raw_data),
            ('forecast_jsons', self.cleanup_forecast_jsons),
            ('bratislava_data', self.cleanup_bratislava_data)
        ]
        
        for op_name, op_func in operations:
            try:
                self.log(f"🔄 Running {op_name} cleanup...")
                op_stats = op_func()
                total_stats['operations'][op_name] = op_stats
                
                # Accumulate totals
                total_stats['total_files_deleted'] += op_stats.get('files_deleted', 0)
                total_stats['total_directories_deleted'] += op_stats.get('directories_deleted', 0)
                total_stats['total_space_freed_bytes'] += op_stats.get('space_freed_bytes', 0)
                total_stats['errors'].extend(op_stats.get('errors', []))
                
            except Exception as e:
                error_msg = f"Error in {op_name} cleanup: {e}"
                self.log(error_msg, "ERROR")
                total_stats['errors'].append(error_msg)
        
        # Calculate duration
        total_stats['duration_seconds'] = round(time.time() - start_time, 2)
        
        # Summary log
        total_space_mb = total_stats['total_space_freed_bytes'] / (1024**2)
        self.log(f"🎉 Cleanup completed in {total_stats['duration_seconds']}s: "
                f"{total_stats['total_files_deleted']} files and {total_stats['total_directories_deleted']} directories deleted, "
                f"{total_space_mb:.1f} MB freed")
        
        if total_stats['errors']:
            self.log(f"⚠️  {len(total_stats['errors'])} errors occurred during cleanup", "WARNING")
        
        return total_stats
    
    def get_data_status(self) -> Dict:
        """Get current status of weather data directories."""
        status = {
            'directories': {},
            'total_files': 0,
            'total_size_mb': 0
        }
        
        directories = [
            ('raw', self.raw_dir),
            ('weather', self.weather_dir),
            ('bratislava', self.bratislava_dir)
        ]
        
        for dir_name, dir_path in directories:
            if dir_path.exists():
                files = list(dir_path.rglob('*'))
                file_count = len([f for f in files if f.is_file()])
                total_size = sum(f.stat().st_size for f in files if f.is_file())
                
                status['directories'][dir_name] = {
                    'exists': True,
                    'files': file_count,
                    'size_mb': round(total_size / (1024**2), 2)
                }
                
                status['total_files'] += file_count
                status['total_size_mb'] += total_size / (1024**2)
            else:
                status['directories'][dir_name] = {
                    'exists': False,
                    'files': 0,
                    'size_mb': 0
                }
        
        status['total_size_mb'] = round(status['total_size_mb'], 2)
        return status


def cleanup_weather_data(retention_hours: int = 12) -> Dict:
    """
    Convenience function to perform weather data cleanup.
    
    Args:
        retention_hours: Hours to keep data (default: 12)
        
    Returns:
        Dictionary with cleanup statistics
    """
    cleanup = WeatherCleanup(retention_hours)
    return cleanup.cleanup_old_data()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Weather Data Cleanup')
    parser.add_argument('--hours', type=int, default=12, 
                       help='Hours of data to keep (default: 12)')
    parser.add_argument('--status', action='store_true',
                       help='Show data status without cleanup')
    
    args = parser.parse_args()
    
    cleanup = WeatherCleanup(retention_hours=args.hours)
    
    if args.status:
        status = cleanup.get_data_status()
        print("\n📊 Weather Data Status")
        print("======================")
        print(f"Total files: {status['total_files']}")
        print(f"Total size: {status['total_size_mb']:.1f} MB")
        print("\nBy directory:")
        for dir_name, dir_info in status['directories'].items():
            if dir_info['exists']:
                print(f"  {dir_name}: {dir_info['files']} files, {dir_info['size_mb']:.1f} MB")
            else:
                print(f"  {dir_name}: (doesn't exist)")
    else:
        cleanup.cleanup_old_data()