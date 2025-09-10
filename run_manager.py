"""
Run Management Database Operations
=================================

This module provides database operations for managing ICON-RUC forecast runs,
files, and storage in a hierarchical structure.
"""

import sqlite3
import json
import os
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import hashlib


class RunManager:
    """Manager class for ICON-RUC run database operations."""
    
    def __init__(self, db_path: str = "runs_index.db"):
        self.db_path = Path(db_path)
        self.base_dir = Path("data/runs")
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self._init_database()
    
    def _init_database(self):
        """Initialize the database with schema."""
        schema_file = Path(__file__).parent / "db_schema.sql"
        
        with sqlite3.connect(self.db_path) as conn:
            if schema_file.exists():
                with open(schema_file, 'r') as f:
                    conn.executescript(f.read())
            else:
                # Fallback basic schema if file doesn't exist
                self._create_basic_schema(conn)
    
    def _create_basic_schema(self, conn: sqlite3.Connection):
        """Create basic schema if SQL file not found."""
        conn.execute("""
            CREATE TABLE IF NOT EXISTS runs (
                run_id TEXT PRIMARY KEY,
                run_datetime TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                file_count INTEGER DEFAULT 0,
                total_size_bytes INTEGER DEFAULT 0,
                output_directory TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS files (
                file_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                ensemble TEXT NOT NULL,
                timestep TEXT NOT NULL,
                filename TEXT NOT NULL,
                filepath TEXT NOT NULL,
                file_size_bytes INTEGER DEFAULT 0,
                FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
            )
        """)
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_outputs (
                output_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT NOT NULL,
                output_type TEXT NOT NULL,
                filepath TEXT NOT NULL,
                file_size_bytes INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
            )
        """)
    
    def create_run_structure(self, run_datetime: datetime) -> Path:
        """
        Create hierarchical directory structure for a run.
        
        Args:
            run_datetime: Forecast run initialization datetime
            
        Returns:
            Path to the run directory
        """
        run_dir = self.base_dir / run_datetime.strftime("%Y/%m/%d/%H")
        
        # Create directory structure
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "raw").mkdir(exist_ok=True)
        (run_dir / "processed").mkdir(exist_ok=True)
        
        # Create ensemble directories
        for i in range(1, 21):  # Assuming 20 ensemble members
            ensemble_dir = run_dir / "raw" / f"e{i:02d}"
            ensemble_dir.mkdir(exist_ok=True)
        
        return run_dir
    
    def register_run(self, run_datetime: datetime, ensembles: List[str] = None, 
                    forecast_steps: List[str] = None) -> str:
        """
        Register a new forecast run in the database.
        
        Args:
            run_datetime: Forecast run initialization datetime
            ensembles: List of ensemble members
            forecast_steps: List of forecast time steps
            
        Returns:
            run_id: Unique identifier for the run
        """
        run_id = run_datetime.strftime("%Y%m%d%H")
        run_dir = self.create_run_structure(run_datetime)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO runs 
                (run_id, run_datetime, status, output_directory, ensembles, forecast_steps)
                VALUES (?, ?, 'pending', ?, ?, ?)
            """, (
                run_id,
                run_datetime.isoformat(),
                str(run_dir),
                json.dumps(ensembles) if ensembles else None,
                json.dumps(forecast_steps) if forecast_steps else None
            ))
        
        return run_id
    
    def update_run_status(self, run_id: str, status: str, **kwargs):
        """Update run status and optional fields."""
        fields = ["status = ?"]
        values = [status]
        
        for key, value in kwargs.items():
            if key in ['download_start_time', 'download_end_time', 'file_count', 'total_size_bytes']:
                fields.append(f"{key} = ?")
                values.append(value)
        
        values.append(run_id)
        
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(f"""
                UPDATE runs 
                SET {', '.join(fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE run_id = ?
            """, values)
    
    def register_file(self, run_id: str, ensemble: str, timestep: str, 
                     filepath: Path) -> int:
        """
        Register a downloaded file in the database.
        
        Args:
            run_id: Run identifier
            ensemble: Ensemble member (e01, e02, etc.)
            timestep: Forecast timestep (PT000H00M, etc.)
            filepath: Full path to the file
            
        Returns:
            file_id: Database ID of the registered file
        """
        file_size = filepath.stat().st_size if filepath.exists() else 0
        checksum = self._calculate_checksum(filepath) if filepath.exists() else None
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO files 
                (run_id, ensemble, timestep, filename, filepath, file_size_bytes, checksum, download_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """, (
                run_id, ensemble, timestep, 
                filepath.name, str(filepath), file_size, checksum
            ))
            return cursor.lastrowid
    
    def register_processed_output(self, run_id: str, location: str, 
                                output_type: str, filepath: Path,
                                processing_time: Optional[float] = None) -> int:
        """Register a processed output file."""
        file_size = filepath.stat().st_size if filepath.exists() else 0
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("""
                INSERT INTO processed_outputs 
                (run_id, location, output_type, filepath, file_size_bytes, processing_time_seconds)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (run_id, location, output_type, str(filepath), file_size, processing_time))
            return cursor.lastrowid
    
    def get_all_runs(self) -> List[Dict]:
        """Get all runs with summary information."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT 
                    r.run_id,
                    r.run_datetime,
                    r.status,
                    r.file_count,
                    ROUND(r.total_size_bytes / 1024.0 / 1024.0, 2) AS total_size_mb,
                    COUNT(po.output_id) AS processed_outputs,
                    r.output_directory,
                    r.created_at,
                    r.updated_at
                FROM runs r
                LEFT JOIN processed_outputs po ON r.run_id = po.run_id
                GROUP BY r.run_id
                ORDER BY r.run_datetime DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_run_details(self, run_id: str) -> Optional[Dict]:
        """Get detailed information about a specific run."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get run info
            run_cursor = conn.execute("""
                SELECT * FROM runs WHERE run_id = ?
            """, (run_id,))
            run = run_cursor.fetchone()
            
            if not run:
                return None
            
            run_dict = dict(run)
            
            # Get files info
            files_cursor = conn.execute("""
                SELECT ensemble, COUNT(*) as file_count, 
                       SUM(file_size_bytes) as total_size,
                       MIN(download_time) as first_download,
                       MAX(download_time) as last_download
                FROM files 
                WHERE run_id = ?
                GROUP BY ensemble
                ORDER BY ensemble
            """, (run_id,))
            run_dict['ensembles'] = [dict(row) for row in files_cursor.fetchall()]
            
            # Get processed outputs
            outputs_cursor = conn.execute("""
                SELECT * FROM processed_outputs WHERE run_id = ?
                ORDER BY created_at DESC
            """, (run_id,))
            run_dict['processed_outputs'] = [dict(row) for row in outputs_cursor.fetchall()]
            
            return run_dict
    
    def delete_run(self, run_id: str, delete_files: bool = True) -> Dict:
        """
        Delete a run and optionally its files.
        
        Args:
            run_id: Run identifier
            delete_files: Whether to delete physical files
            
        Returns:
            Dictionary with deletion statistics
        """
        stats = {
            'files_deleted': 0,
            'space_freed_bytes': 0,
            'directories_removed': [],
            'errors': []
        }
        
        # Get run info and files
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get run directory
            run_cursor = conn.execute("SELECT output_directory, total_size_bytes FROM runs WHERE run_id = ?", (run_id,))
            run_info = run_cursor.fetchone()
            
            if not run_info:
                stats['errors'].append(f"Run {run_id} not found")
                return stats
            
            stats['space_freed_bytes'] = run_info['total_size_bytes'] or 0
            
            if delete_files and run_info['output_directory']:
                run_dir = Path(run_info['output_directory'])
                if run_dir.exists():
                    try:
                        # Count files before deletion
                        file_count = sum(1 for _ in run_dir.rglob('*') if _.is_file())
                        stats['files_deleted'] = file_count
                        
                        # Delete directory and all contents
                        shutil.rmtree(run_dir)
                        stats['directories_removed'].append(str(run_dir))
                        
                    except Exception as e:
                        stats['errors'].append(f"Error deleting {run_dir}: {e}")
            
            # Delete from database
            conn.execute("DELETE FROM runs WHERE run_id = ?", (run_id,))
        
        # Record cleanup in history
        self._record_cleanup('manual', [run_id], stats)
        
        return stats
    
    def cleanup_old_runs(self, keep_last_n: Optional[int] = None,
                        delete_older_than_days: Optional[int] = None,
                        max_total_size_gb: Optional[float] = None) -> Dict:
        """
        Clean up old runs based on retention policies.
        
        Args:
            keep_last_n: Keep the last N runs
            delete_older_than_days: Delete runs older than N days
            max_total_size_gb: Keep total storage under N GB
            
        Returns:
            Dictionary with cleanup statistics
        """
        runs_to_delete = []
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            
            # Get all runs ordered by datetime
            all_runs = conn.execute("""
                SELECT run_id, run_datetime, total_size_bytes
                FROM runs 
                ORDER BY run_datetime DESC
            """).fetchall()
            
            if keep_last_n and len(all_runs) > keep_last_n:
                runs_to_delete.extend([run['run_id'] for run in all_runs[keep_last_n:]])
            
            if delete_older_than_days:
                cutoff_date = datetime.now() - timedelta(days=delete_older_than_days)
                for run in all_runs:
                    run_date = datetime.fromisoformat(run['run_datetime'])
                    if run_date < cutoff_date and run['run_id'] not in runs_to_delete:
                        runs_to_delete.append(run['run_id'])
            
            if max_total_size_gb:
                max_size_bytes = max_total_size_gb * 1024 * 1024 * 1024
                current_size = 0
                for run in all_runs:
                    current_size += run['total_size_bytes'] or 0
                    if current_size > max_size_bytes and run['run_id'] not in runs_to_delete:
                        runs_to_delete.append(run['run_id'])
        
        # Delete selected runs
        total_stats = {
            'runs_deleted': 0,
            'files_deleted': 0,
            'space_freed_bytes': 0,
            'directories_removed': [],
            'errors': []
        }
        
        for run_id in runs_to_delete:
            run_stats = self.delete_run(run_id, delete_files=True)
            total_stats['runs_deleted'] += 1
            total_stats['files_deleted'] += run_stats['files_deleted']
            total_stats['space_freed_bytes'] += run_stats['space_freed_bytes']
            total_stats['directories_removed'].extend(run_stats['directories_removed'])
            total_stats['errors'].extend(run_stats['errors'])
        
        # Record cleanup in history
        criteria = {
            'keep_last_n': keep_last_n,
            'delete_older_than_days': delete_older_than_days,
            'max_total_size_gb': max_total_size_gb
        }
        self._record_cleanup('automatic', runs_to_delete, total_stats, criteria)
        
        return total_stats
    
    def get_storage_summary(self) -> Dict:
        """Get overall storage usage summary."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_runs,
                    SUM(file_count) as total_files,
                    SUM(total_size_bytes) as total_raw_bytes,
                    ROUND(SUM(total_size_bytes) / 1024.0 / 1024.0, 2) as total_raw_mb,
                    ROUND(SUM(total_size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) as total_raw_gb,
                    MIN(run_datetime) as oldest_run,
                    MAX(run_datetime) as newest_run
                FROM runs
                WHERE status = 'complete'
            """)
            
            summary = dict(cursor.fetchone())
            
            # Get processed outputs size
            processed_cursor = conn.execute("""
                SELECT 
                    COUNT(*) as processed_files,
                    SUM(file_size_bytes) as processed_bytes,
                    ROUND(SUM(file_size_bytes) / 1024.0 / 1024.0, 2) as processed_mb
                FROM processed_outputs
            """)
            
            processed_info = dict(processed_cursor.fetchone())
            summary.update(processed_info)
            
            return summary
    
    def _calculate_checksum(self, filepath: Path) -> str:
        """Calculate SHA256 checksum of a file."""
        if not filepath.exists():
            return None
        
        hash_sha256 = hashlib.sha256()
        with open(filepath, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_sha256.update(chunk)
        return hash_sha256.hexdigest()
    
    def _record_cleanup(self, cleanup_type: str, runs_deleted: List[str], 
                       stats: Dict, criteria: Dict = None):
        """Record cleanup operation in history."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO cleanup_history 
                (cleanup_type, runs_deleted, files_deleted, space_freed_bytes, cleanup_criteria)
                VALUES (?, ?, ?, ?, ?)
            """, (
                cleanup_type,
                len(runs_deleted),
                stats['files_deleted'],
                stats['space_freed_bytes'],
                json.dumps(criteria) if criteria else json.dumps({"runs": runs_deleted})
            ))
    
    def get_cleanup_history(self, limit: int = 50) -> List[Dict]:
        """Get cleanup operation history."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM cleanup_history 
                ORDER BY cleanup_time DESC 
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]