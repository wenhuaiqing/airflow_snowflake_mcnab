-- Create contractor_performance table if it does not exist
CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.contractor_performance (
    contractor_id STRING PRIMARY KEY,
    contractor_name STRING,
    contractor_type STRING,
    total_projects NUMBER,
    total_activities NUMBER,
    total_hours_worked NUMBER,
    total_cost NUMBER,
    avg_cost_per_hour NUMBER,
    avg_activity_cost NUMBER,
    projects_completed NUMBER,
    projects_in_progress NUMBER,
    completion_rate_percentage NUMBER,
    years_experience NUMBER,
    license_type STRING,
    registration_date DATE
);
