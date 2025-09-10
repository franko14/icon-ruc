-- Simple database migration: Add multi-variable support
-- Creates tables and columns step by step

-- 1. Create weather_variables table
CREATE TABLE IF NOT EXISTS weather_variables (
    variable_id TEXT PRIMARY KEY,
    variable_name TEXT NOT NULL,
    description TEXT,
    units TEXT NOT NULL,
    processing_type TEXT NOT NULL,
    dwd_parameter TEXT NOT NULL,
    base_url TEXT NOT NULL,
    thresholds TEXT,
    aggregations TEXT,
    percentiles TEXT,
    color_scheme TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- 2. Add columns to runs table
ALTER TABLE runs ADD COLUMN variables TEXT;
ALTER TABLE runs ADD COLUMN variable_count INTEGER DEFAULT 1;

-- 3. Add variable_id to files table
ALTER TABLE files ADD COLUMN variable_id TEXT;

-- 4. Add variable_id to processed_outputs table  
ALTER TABLE processed_outputs ADD COLUMN variable_id TEXT;

-- Migration continues in Python code for data population