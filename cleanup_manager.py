#!/usr/bin/env python3
"""
Automatic Cleanup Manager for ICON-RUC Data
===========================================

Provides automatic cleanup functionality with configurable retention policies.
Can be run as a standalone script or integrated into the API server.

Usage: 
  python cleanup_manager.py [--config config.json]
  
Default retention policy:
  - Keep last 5 runs
  - Delete runs older than 7 days  
  - Maximum total storage: 10 GB
"""

import os
import sys
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Optional, List
import threading
import signal

# Add current directory to path for imports
sys.path.append(os.path.dirname(__file__))

from run_manager import RunManager


class CleanupManager:
    """Manages automatic cleanup of forecast runs based on retention policies."""
    
    def __init__(self, config_file: Optional[str] = None):
        """
        Initialize cleanup manager with configuration.
        
        Args:
            config_file: Path to JSON configuration file
        """
        self.run_manager = RunManager()
        self.config = self._load_config(config_file)
        self.running = False
        self.cleanup_thread = None
        
    def _load_config(self, config_file: Optional[str]) -> Dict:
        """Load configuration from file or use defaults."""
        default_config = {
            "retention_policies": {
                "keep_last_n_runs": 5,
                "delete_older_than_days": 7,
                "max_total_size_gb": 10.0
            },
            "cleanup_schedule": {
                "enabled": True,
                "interval_hours": 6,
                "run_at_startup": True
            },
            "safety_limits": {
                "min_runs_to_keep": 2,
                "max_runs_to_delete_per_cleanup": 20
            },
            "notifications": {
                "log_cleanup_actions": True,
                "log_storage_warnings": True,
                "storage_warning_threshold_gb": 15.0
            }
        }
        
        if config_file and Path(config_file).exists():
            try:
                with open(config_file, 'r') as f:
                    user_config = json.load(f)
                
                # Merge user config with defaults
                def merge_dict(default, user):
                    result = default.copy()
                    for key, value in user.items():
                        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                            result[key] = merge_dict(result[key], value)
                        else:
                            result[key] = value
                    return result
                
                return merge_dict(default_config, user_config)
                
            except Exception as e:
                self.log(f"⚠️ Error loading config file {config_file}: {e}")
                self.log("📄 Using default configuration")
        
        return default_config
    
    def log(self, message: str):
        """Log message with timestamp."""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[CLEANUP {timestamp}] {message}")
    
    def get_storage_status(self) -> Dict:
        """Get current storage usage status."""
        try:
            summary = self.run_manager.get_storage_summary()
            return {
                'success': True,
                'total_runs': summary.get('total_runs', 0),
                'total_size_gb': summary.get('total_raw_gb', 0.0),
                'total_files': summary.get('total_files', 0),
                'oldest_run': summary.get('oldest_run'),
                'newest_run': summary.get('newest_run')
            }
        except Exception as e:
            return {
                'success': False,
                'error': str(e)
            }
    
    def should_cleanup(self) -> tuple[bool, List[str]]:
        """
        Check if cleanup should be performed based on current storage status.
        
        Returns:
            Tuple of (should_cleanup, reasons)
        """
        reasons = []
        status = self.get_storage_status()
        
        if not status['success']:
            return False, [f"Could not get storage status: {status['error']}"]
        
        policies = self.config['retention_policies']
        notifications = self.config['notifications']
        
        # Check storage size limit
        current_size_gb = status['total_size_gb']
        max_size_gb = policies['max_total_size_gb']
        
        if current_size_gb > max_size_gb:
            reasons.append(f"Storage usage ({current_size_gb:.2f} GB) exceeds limit ({max_size_gb} GB)")
        
        # Check storage warning threshold
        warning_threshold = notifications['storage_warning_threshold_gb']
        if current_size_gb > warning_threshold:
            if notifications['log_storage_warnings']:
                self.log(f"⚠️ Storage usage approaching limit: {current_size_gb:.2f} GB "
                        f"(warning threshold: {warning_threshold} GB, limit: {max_size_gb} GB)")
        
        # Check number of runs
        total_runs = status['total_runs']
        keep_last_n = policies['keep_last_n_runs']
        min_keep = self.config['safety_limits']['min_runs_to_keep']
        
        if total_runs > keep_last_n and total_runs > min_keep:
            excess_runs = total_runs - keep_last_n
            reasons.append(f"Have {total_runs} runs, policy is to keep last {keep_last_n}")
        
        # Check age of oldest run
        if status['oldest_run']:
            try:
                oldest_date = datetime.fromisoformat(status['oldest_run'])
                cutoff_date = datetime.now() - timedelta(days=policies['delete_older_than_days'])
                
                if oldest_date < cutoff_date:
                    days_old = (datetime.now() - oldest_date).days
                    reasons.append(f"Oldest run is {days_old} days old, policy is {policies['delete_older_than_days']} days")
            
            except Exception as e:
                self.log(f"⚠️ Error parsing oldest run date: {e}")
        
        return len(reasons) > 0, reasons
    
    def perform_cleanup(self) -> Dict:
        """
        Perform cleanup based on retention policies.
        
        Returns:
            Dictionary with cleanup results
        """
        self.log("🧹 Starting cleanup process...")
        
        should_cleanup, reasons = self.should_cleanup()
        
        if not should_cleanup:
            self.log("ℹ️ No cleanup needed based on current policies")
            return {
                'cleanup_performed': False,
                'reason': 'No cleanup criteria met',
                'stats': {}
            }
        
        self.log(f"📋 Cleanup needed: {', '.join(reasons)}")
        
        policies = self.config['retention_policies']
        safety_limits = self.config['safety_limits']
        
        try:
            # Perform cleanup with safety limits
            cleanup_stats = self.run_manager.cleanup_old_runs(
                keep_last_n=max(policies['keep_last_n_runs'], safety_limits['min_runs_to_keep']),
                delete_older_than_days=policies['delete_older_than_days'],
                max_total_size_gb=policies['max_total_size_gb']
            )
            
            # Check if we're within safety limits
            runs_deleted = cleanup_stats['runs_deleted']
            max_deletions = safety_limits['max_runs_to_delete_per_cleanup']
            
            if runs_deleted > max_deletions:
                self.log(f"⚠️ Safety limit exceeded: Would delete {runs_deleted} runs, "
                        f"limit is {max_deletions}")
                return {
                    'cleanup_performed': False,
                    'reason': f'Safety limit exceeded ({runs_deleted} > {max_deletions})',
                    'stats': cleanup_stats
                }
            
            # Log results
            if self.config['notifications']['log_cleanup_actions']:
                space_freed_gb = cleanup_stats['space_freed_bytes'] / (1024**3)
                self.log(f"✅ Cleanup completed: {runs_deleted} runs deleted, "
                        f"{cleanup_stats['files_deleted']} files removed, "
                        f"{space_freed_gb:.2f} GB freed")
                
                if cleanup_stats['errors']:
                    self.log(f"⚠️ Encountered {len(cleanup_stats['errors'])} errors during cleanup")
                    for error in cleanup_stats['errors'][:3]:  # Show first 3 errors
                        self.log(f"   💥 {error}")
            
            return {
                'cleanup_performed': True,
                'stats': cleanup_stats,
                'space_freed_gb': space_freed_gb
            }
            
        except Exception as e:
            self.log(f"❌ Cleanup failed: {e}")
            return {
                'cleanup_performed': False,
                'reason': f'Cleanup failed: {e}',
                'stats': {}
            }
    
    def cleanup_loop(self):
        """Main cleanup loop running in background thread."""
        self.log("🚀 Starting automatic cleanup service")
        
        schedule_config = self.config['cleanup_schedule']
        interval_seconds = schedule_config['interval_hours'] * 3600
        
        # Run cleanup at startup if configured
        if schedule_config.get('run_at_startup', False):
            self.log("🏃 Running initial cleanup at startup...")
            try:
                self.perform_cleanup()
            except Exception as e:
                self.log(f"❌ Initial cleanup failed: {e}")
        
        # Main cleanup loop
        while self.running:
            try:
                # Wait for next cleanup interval
                for _ in range(int(interval_seconds)):
                    if not self.running:
                        break
                    time.sleep(1)
                
                if self.running:
                    self.perform_cleanup()
                    
            except Exception as e:
                self.log(f"❌ Error in cleanup loop: {e}")
                # Sleep for a short time before retrying
                time.sleep(60)
    
    def start_automatic_cleanup(self):
        """Start automatic cleanup in background thread."""
        if self.running:
            self.log("⚠️ Cleanup service already running")
            return
        
        if not self.config['cleanup_schedule']['enabled']:
            self.log("ℹ️ Automatic cleanup is disabled in configuration")
            return
        
        self.running = True
        self.cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
        self.cleanup_thread.start()
        
        self.log(f"✅ Automatic cleanup service started (interval: {self.config['cleanup_schedule']['interval_hours']} hours)")
    
    def stop_automatic_cleanup(self):
        """Stop automatic cleanup."""
        if not self.running:
            return
        
        self.log("🛑 Stopping automatic cleanup service...")
        self.running = False
        
        if self.cleanup_thread and self.cleanup_thread.is_alive():
            self.cleanup_thread.join(timeout=5)
        
        self.log("✅ Automatic cleanup service stopped")
    
    def save_config(self, config_file: str):
        """Save current configuration to file."""
        try:
            with open(config_file, 'w') as f:
                json.dump(self.config, f, indent=2)
            self.log(f"💾 Configuration saved to {config_file}")
        except Exception as e:
            self.log(f"❌ Failed to save configuration: {e}")


def signal_handler(signum, frame, cleanup_manager):
    """Handle shutdown signals gracefully."""
    print(f"\n🛑 Received signal {signum}, shutting down...")
    cleanup_manager.stop_automatic_cleanup()
    sys.exit(0)


def main():
    """Main function for standalone usage."""
    parser = argparse.ArgumentParser(description='ICON-RUC Automatic Cleanup Manager')
    parser.add_argument('--config', type=str, help='Path to configuration JSON file')
    parser.add_argument('--once', action='store_true', 
                       help='Run cleanup once and exit (no automatic mode)')
    parser.add_argument('--status', action='store_true', 
                       help='Show current storage status and exit')
    parser.add_argument('--save-config', type=str,
                       help='Save default configuration to specified file')
    
    args = parser.parse_args()
    
    # Initialize cleanup manager
    try:
        cleanup_manager = CleanupManager(args.config)
    except Exception as e:
        print(f"❌ Failed to initialize cleanup manager: {e}")
        return 1
    
    # Handle --save-config
    if args.save_config:
        cleanup_manager.save_config(args.save_config)
        return 0
    
    # Handle --status
    if args.status:
        print("📊 Current Storage Status")
        print("========================")
        
        status = cleanup_manager.get_storage_status()
        if status['success']:
            print(f"Total runs: {status['total_runs']}")
            print(f"Total files: {status['total_files']}")
            print(f"Total size: {status['total_size_gb']:.2f} GB")
            print(f"Oldest run: {status['oldest_run'] or 'N/A'}")
            print(f"Newest run: {status['newest_run'] or 'N/A'}")
            
            should_cleanup, reasons = cleanup_manager.should_cleanup()
            print(f"\nCleanup needed: {'Yes' if should_cleanup else 'No'}")
            if reasons:
                for reason in reasons:
                    print(f"  • {reason}")
        else:
            print(f"❌ Error getting status: {status['error']}")
            return 1
        
        return 0
    
    # Handle --once (run cleanup once)
    if args.once:
        result = cleanup_manager.perform_cleanup()
        return 0 if result['cleanup_performed'] or 'No cleanup needed' in result.get('reason', '') else 1
    
    # Run automatic cleanup service
    print("🔄 ICON-RUC Automatic Cleanup Manager")
    print("====================================")
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, lambda s, f: signal_handler(s, f, cleanup_manager))
    signal.signal(signal.SIGTERM, lambda s, f: signal_handler(s, f, cleanup_manager))
    
    cleanup_manager.start_automatic_cleanup()
    
    try:
        # Keep main thread alive
        while cleanup_manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        cleanup_manager.stop_automatic_cleanup()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())