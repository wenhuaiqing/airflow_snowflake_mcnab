-- Create project_cost_analysis table if it does not exist
CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.project_cost_analysis (
    project_id STRING PRIMARY KEY,
    project_name STRING,
    project_type STRING,
    project_status STRING,
    total_activities NUMBER,
    total_material_cost NUMBER,
    total_labor_cost NUMBER,
    total_cost NUMBER,
    project_budget NUMBER,
    budget_utilization_percentage NUMBER,
    cost_per_sqft NUMBER,
    avg_activity_cost NUMBER,
    project_start_date DATE,
    estimated_completion DATE,
    location STRING,
    client_name STRING
);