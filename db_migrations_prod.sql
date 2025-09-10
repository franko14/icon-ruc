-- Production database schema for ICON-RUC weather data processing
-- Enhanced with progress tracking, job management, and monitoring

-- Drop existing tables in correct order to handle dependencies
DROP TABLE IF EXISTS processing_substeps CASCADE;
DROP TABLE IF EXISTS processing_steps CASCADE;
DROP TABLE IF EXISTS processing_jobs CASCADE;
DROP TABLE IF EXISTS run_variables CASCADE;
DROP TABLE IF EXISTS variable_configs CASCADE;
DROP TABLE IF EXISTS runs CASCADE;

-- Main runs table - tracks forecast runs
CREATE TABLE runs (
    id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    run_hour INTEGER NOT NULL CHECK (run_hour >= 0 AND run_hour <= 23),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'downloading', 'processing', 'completed', 'failed')),
    total_files INTEGER DEFAULT 0,
    downloaded_files INTEGER DEFAULT 0,
    processed_files INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    error_message TEXT NULL,
    metadata JSONB DEFAULT '{}',
    
    -- Add performance tracking
    download_duration_seconds INTEGER NULL,
    processing_duration_seconds INTEGER NULL,
    total_size_mb DECIMAL(10,2) NULL,
    
    UNIQUE(run_date, run_hour)
);

-- Variable configurations table
CREATE TABLE variable_configs (
    id SERIAL PRIMARY KEY,
    variable_name TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    unit TEXT NOT NULL,
    description TEXT,
    is_enabled BOOLEAN DEFAULT true,
    processing_order INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Configuration options
    config_options JSONB DEFAULT '{}'
);

-- Run-specific variable tracking
CREATE TABLE run_variables (
    id SERIAL PRIMARY KEY,
    run_id INTEGER REFERENCES runs(id) ON DELETE CASCADE,
    variable_id INTEGER REFERENCES variable_configs(id) ON DELETE CASCADE,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'downloading', 'processing', 'completed', 'failed', 'skipped')),
    total_files INTEGER DEFAULT 0,
    downloaded_files INTEGER DEFAULT 0,
    processed_files INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    error_message TEXT NULL,
    
    -- Performance metrics
    download_duration_seconds INTEGER NULL,
    processing_duration_seconds INTEGER NULL,
    file_size_mb DECIMAL(10,2) NULL,
    
    UNIQUE(run_id, variable_id)
);

-- Processing jobs table - tracks individual processing jobs
CREATE TABLE processing_jobs (
    id SERIAL PRIMARY KEY,
    job_type TEXT NOT NULL CHECK (job_type IN ('download', 'process', 'full_pipeline')),
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    
    -- Job identification
    run_id INTEGER REFERENCES runs(id) ON DELETE CASCADE,
    variable_ids INTEGER[] DEFAULT '{}',
    location_name TEXT DEFAULT 'Bratislava',
    location_lat DECIMAL(8,5) DEFAULT 48.1486,
    location_lon DECIMAL(8,5) DEFAULT 17.1077,
    
    -- Progress tracking
    total_steps INTEGER DEFAULT 0,
    completed_steps INTEGER DEFAULT 0,
    current_step_name TEXT NULL,
    progress_percentage DECIMAL(5,2) DEFAULT 0.00,
    
    -- Timing
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    estimated_completion TIMESTAMP NULL,
    
    -- Error handling
    error_message TEXT NULL,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    
    -- Metadata and configuration
    job_config JSONB DEFAULT '{}',
    result_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Processing steps table - tracks major steps within a job
CREATE TABLE processing_steps (
    id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES processing_jobs(id) ON DELETE CASCADE,
    step_name TEXT NOT NULL,
    step_order INTEGER NOT NULL,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),
    
    -- Progress within this step
    total_substeps INTEGER DEFAULT 0,
    completed_substeps INTEGER DEFAULT 0,
    progress_percentage DECIMAL(5,2) DEFAULT 0.00,
    
    -- Timing
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    duration_seconds INTEGER NULL,
    
    -- Results
    error_message TEXT NULL,
    step_metadata JSONB DEFAULT '{}',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(job_id, step_name)
);

-- Processing substeps table - tracks detailed operations within steps
CREATE TABLE processing_substeps (
    id SERIAL PRIMARY KEY,
    step_id INTEGER REFERENCES processing_steps(id) ON DELETE CASCADE,
    substep_name TEXT NOT NULL,
    substep_order INTEGER NOT NULL,
    status TEXT DEFAULT 'pending' CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),
    
    -- Details
    description TEXT,
    progress_percentage DECIMAL(5,2) DEFAULT 0.00,
    
    -- Timing
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    duration_seconds INTEGER NULL,
    
    -- Results
    error_message TEXT NULL,
    result_data JSONB DEFAULT '{}',
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE(step_id, substep_name)
);

-- Create indexes for performance
CREATE INDEX idx_runs_date_hour ON runs(run_date, run_hour);
CREATE INDEX idx_runs_status ON runs(status);
CREATE INDEX idx_runs_created_at ON runs(created_at);

CREATE INDEX idx_run_variables_run_id ON run_variables(run_id);
CREATE INDEX idx_run_variables_status ON run_variables(status);
CREATE INDEX idx_run_variables_variable_id ON run_variables(variable_id);

CREATE INDEX idx_processing_jobs_status ON processing_jobs(status);
CREATE INDEX idx_processing_jobs_job_type ON processing_jobs(job_type);
CREATE INDEX idx_processing_jobs_run_id ON processing_jobs(run_id);
CREATE INDEX idx_processing_jobs_created_at ON processing_jobs(created_at);

CREATE INDEX idx_processing_steps_job_id ON processing_steps(job_id);
CREATE INDEX idx_processing_steps_status ON processing_steps(status);
CREATE INDEX idx_processing_steps_order ON processing_steps(job_id, step_order);

CREATE INDEX idx_processing_substeps_step_id ON processing_substeps(step_id);
CREATE INDEX idx_processing_substeps_status ON processing_substeps(status);
CREATE INDEX idx_processing_substeps_order ON processing_substeps(step_id, substep_order);

-- Create update triggers for timestamp fields
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_runs_updated_at BEFORE UPDATE ON runs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    
CREATE TRIGGER update_run_variables_updated_at BEFORE UPDATE ON run_variables
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    
CREATE TRIGGER update_processing_jobs_updated_at BEFORE UPDATE ON processing_jobs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    
CREATE TRIGGER update_processing_steps_updated_at BEFORE UPDATE ON processing_steps
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
    
CREATE TRIGGER update_processing_substeps_updated_at BEFORE UPDATE ON processing_substeps
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert default variable configurations
INSERT INTO variable_configs (variable_name, display_name, unit, description, processing_order) VALUES
('TOT_PREC', 'Total Precipitation', 'mm/h', 'Accumulated precipitation deaccumulated to hourly rates', 1),
('WIND_10M', 'Wind Speed 10m', 'm/s', '10-meter wind speed', 2),
('T_2M', 'Temperature 2m', '°C', '2-meter air temperature', 3),
('RELHUM_2M', 'Relative Humidity 2m', '%', '2-meter relative humidity', 4),
('PS', 'Surface Pressure', 'hPa', 'Surface pressure', 5);

-- Create views for easier querying
CREATE VIEW job_progress AS
SELECT 
    j.id,
    j.job_type,
    j.status,
    j.location_name,
    j.progress_percentage,
    j.total_steps,
    j.completed_steps,
    j.current_step_name,
    j.started_at,
    j.estimated_completion,
    r.run_date,
    r.run_hour,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - j.started_at)) as elapsed_seconds
FROM processing_jobs j
LEFT JOIN runs r ON j.run_id = r.id
WHERE j.status IN ('pending', 'running');

CREATE VIEW recent_jobs AS
SELECT 
    j.id,
    j.job_type,
    j.status,
    j.location_name,
    j.progress_percentage,
    j.started_at,
    j.completed_at,
    j.error_message,
    r.run_date,
    r.run_hour,
    CASE 
        WHEN j.completed_at IS NOT NULL THEN 
            EXTRACT(EPOCH FROM (j.completed_at - j.started_at))
        ELSE 
            EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - j.started_at))
    END as duration_seconds
FROM processing_jobs j
LEFT JOIN runs r ON j.run_id = r.id
ORDER BY j.created_at DESC
LIMIT 100;

-- Performance monitoring view
CREATE VIEW system_health AS
SELECT 
    COUNT(CASE WHEN status = 'running' THEN 1 END) as active_jobs,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_jobs_24h,
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed_jobs_24h,
    AVG(CASE WHEN status = 'completed' AND completed_at IS NOT NULL THEN 
        EXTRACT(EPOCH FROM (completed_at - started_at)) END) as avg_completion_time_seconds,
    MAX(created_at) as latest_job_time
FROM processing_jobs 
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '24 hours';

COMMENT ON TABLE runs IS 'Main forecast runs tracking';
COMMENT ON TABLE processing_jobs IS 'Individual processing jobs with progress tracking';
COMMENT ON TABLE processing_steps IS 'Major steps within processing jobs';
COMMENT ON TABLE processing_substeps IS 'Detailed substeps for granular progress tracking';
COMMENT ON VIEW job_progress IS 'Current progress of active jobs';
COMMENT ON VIEW recent_jobs IS 'Recent job history with performance metrics';
COMMENT ON VIEW system_health IS 'System health metrics for monitoring';