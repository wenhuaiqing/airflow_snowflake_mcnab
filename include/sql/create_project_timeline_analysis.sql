-- Create project timeline analysis table
-- Tracks project progress, duration, and timeline metrics

CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.project_timeline_analysis (
    project_id STRING PRIMARY KEY,
    project_name STRING,
    project_type STRING,
    project_status STRING,
    start_date DATE,
    estimated_completion DATE,
    actual_completion_date DATE,
    project_duration_days INT,
    estimated_duration_days INT,
    timeline_variance_days INT,
    timeline_variance_percentage NUMBER,
    activities_completed INT,
    total_activities INT,
    completion_percentage NUMBER,
    avg_activities_per_day NUMBER,
    last_activity_date DATE,
    location STRING,
    client_name STRING
);
