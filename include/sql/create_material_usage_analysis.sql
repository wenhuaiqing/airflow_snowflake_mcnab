-- Create material_usage_analysis table if it does not exist
CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.material_usage_analysis (
    material_id STRING PRIMARY KEY,
    material_name STRING,
    material_category STRING,
    total_quantity_used NUMBER,
    total_cost NUMBER,
    avg_unit_price NUMBER,
    total_projects_used NUMBER,
    total_activities_used NUMBER,
    usage_frequency_percentage NUMBER,
    most_used_in_activity_type STRING,
    avg_quantity_per_activity NUMBER,
    unit STRING
);