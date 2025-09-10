-- Database schema for ICON-RUC run management system
-- SQLite database schema for tracking forecast runs and files

-- Main runs table - tracks each forecast run
CREATE TABLE IF NOT EXISTS runs (
    run_id TEXT PRIMARY KEY,                    -- Unique run identifier (YYYYMMDDHH format)
    run_datetime TEXT NOT NULL,                 -- ISO format datetime of the run
    download_start_time TEXT,                   -- When download started
    download_end_time TEXT,                     -- When download completed
    status TEXT NOT NULL DEFAULT 'pending',    -- pending, downloading, processing, complete, error
    file_count INTEGER DEFAULT 0,              -- Number of files in this run
    total_size_bytes INTEGER DEFAULT 0,        -- Total size of all files in bytes
    ensembles TEXT,                            -- JSON array of ensemble members
    forecast_steps TEXT,                       -- JSON array of forecast steps
    location TEXT DEFAULT 'bratislava',       -- Target location name
    output_directory TEXT,                     -- Base directory path for this run
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Files table - tracks individual GRIB2 files within each run
CREATE TABLE IF NOT EXISTS files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,                      -- Reference to runs table
    ensemble TEXT NOT NULL,                    -- Ensemble member (e01, e02, etc.)
    timestep TEXT NOT NULL,                    -- Forecast timestep (PT000H00M, etc.)
    filename TEXT NOT NULL,                    -- Original filename
    filepath TEXT NOT NULL,                    -- Full path to file
    file_size_bytes INTEGER DEFAULT 0,        -- Individual file size
    checksum TEXT,                             -- File checksum for integrity
    download_time TEXT,                        -- When this file was downloaded
    FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
);

-- Processed outputs table - tracks processed NetCDF and JSON files
CREATE TABLE IF NOT EXISTS processed_outputs (
    output_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,                      -- Reference to runs table
    location TEXT NOT NULL,                    -- Location name (bratislava, etc.)
    output_type TEXT NOT NULL,                 -- nc, json, csv, etc.
    filepath TEXT NOT NULL,                    -- Full path to processed file
    file_size_bytes INTEGER DEFAULT 0,        -- Size of processed file
    processing_time_seconds REAL,             -- Time taken to process
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (run_id) REFERENCES runs (run_id) ON DELETE CASCADE
);

-- Storage statistics table - for tracking space usage over time
CREATE TABLE IF NOT EXISTS storage_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    total_runs INTEGER NOT NULL,
    total_files INTEGER NOT NULL,
    total_size_bytes INTEGER NOT NULL,
    raw_size_bytes INTEGER NOT NULL,
    processed_size_bytes INTEGER NOT NULL,
    oldest_run_date TEXT,
    newest_run_date TEXT,
    recorded_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Cleanup history table - tracks cleanup operations
CREATE TABLE IF NOT EXISTS cleanup_history (
    cleanup_id INTEGER PRIMARY KEY AUTOINCREMENT,
    cleanup_type TEXT NOT NULL,               -- manual, automatic, retention_policy
    runs_deleted INTEGER DEFAULT 0,
    files_deleted INTEGER DEFAULT 0,
    space_freed_bytes INTEGER DEFAULT 0,
    cleanup_criteria TEXT,                    -- JSON with cleanup parameters
    cleanup_time TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_runs_datetime ON runs (run_datetime);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs (status);
CREATE INDEX IF NOT EXISTS idx_files_run_id ON files (run_id);
CREATE INDEX IF NOT EXISTS idx_files_ensemble ON files (ensemble);
CREATE INDEX IF NOT EXISTS idx_outputs_run_id ON processed_outputs (run_id);
CREATE INDEX IF NOT EXISTS idx_outputs_type ON processed_outputs (output_type);

-- Views for common queries
CREATE VIEW IF NOT EXISTS run_summary AS
SELECT 
    r.run_id,
    r.run_datetime,
    r.status,
    r.file_count,
    ROUND(r.total_size_bytes / 1024.0 / 1024.0, 2) AS total_size_mb,
    COUNT(po.output_id) AS processed_outputs,
    r.created_at,
    r.updated_at
FROM runs r
LEFT JOIN processed_outputs po ON r.run_id = po.run_id
GROUP BY r.run_id
ORDER BY r.run_datetime DESC;

CREATE VIEW IF NOT EXISTS storage_summary AS
SELECT 
    COUNT(*) as total_runs,
    SUM(file_count) as total_files,
    SUM(total_size_bytes) as total_raw_bytes,
    ROUND(SUM(total_size_bytes) / 1024.0 / 1024.0, 2) as total_raw_mb,
    ROUND(SUM(total_size_bytes) / 1024.0 / 1024.0 / 1024.0, 2) as total_raw_gb,
    MIN(run_datetime) as oldest_run,
    MAX(run_datetime) as newest_run
FROM runs
WHERE status = 'complete';

-- Triggers to maintain data consistency
CREATE TRIGGER IF NOT EXISTS update_run_timestamp
    AFTER UPDATE ON runs
    BEGIN
        UPDATE runs SET updated_at = CURRENT_TIMESTAMP WHERE run_id = NEW.run_id;
    END;

CREATE TRIGGER IF NOT EXISTS update_file_count
    AFTER INSERT ON files
    BEGIN
        UPDATE runs 
        SET 
            file_count = (SELECT COUNT(*) FROM files WHERE run_id = NEW.run_id),
            total_size_bytes = (SELECT COALESCE(SUM(file_size_bytes), 0) FROM files WHERE run_id = NEW.run_id),
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = NEW.run_id;
    END;

CREATE TRIGGER IF NOT EXISTS update_file_count_on_delete
    AFTER DELETE ON files
    BEGIN
        UPDATE runs 
        SET 
            file_count = (SELECT COUNT(*) FROM files WHERE run_id = OLD.run_id),
            total_size_bytes = (SELECT COALESCE(SUM(file_size_bytes), 0) FROM files WHERE run_id = OLD.run_id),
            updated_at = CURRENT_TIMESTAMP
        WHERE run_id = OLD.run_id;
    END;