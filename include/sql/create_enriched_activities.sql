-- Create enriched project activities table with joined data and calculated metrics
-- This table combines all construction data for analytics

CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.enriched_activities (
    activity_id STRING,
    project_id STRING,
    project_name STRING,
    project_type STRING,
    project_status STRING,
    contractor_id STRING,
    contractor_name STRING,
    contractor_type STRING,
    material_id STRING,
    material_name STRING,
    material_category STRING,
    activity_type STRING,
    quantity INT,
    unit_price NUMBER,
    unit STRING,
    activity_date TIMESTAMP,
    hours_worked INT,
    cost NUMBER,
    total_material_cost NUMBER,
    labor_cost NUMBER,
    project_budget NUMBER,
    project_location STRING,
    client_name STRING,
    architect STRING,
    CONSTRAINT unique_activity_id UNIQUE (activity_id)
) AS
SELECT 
    pa.activity_id,
    pa.project_id,
    p.project_name,
    p.project_type,
    p.project_status,
    pa.contractor_id,
    c.contractor_name,
    c.contractor_type,
    pa.material_id,
    m.material_name,
    m.material_category,
    pa.activity_type,
    pa.quantity,
    m.unit_price,
    m.unit,
    pa.activity_date,
    pa.hours_worked,
    pa.cost,
    (pa.quantity * m.unit_price) AS total_material_cost,
    (pa.hours_worked * 50) AS labor_cost,  -- Assuming $50/hour labor rate
    p.budget AS project_budget,
    p.location AS project_location,
    p.client_name,
    p.architect
FROM {{ params.db_name }}.{{ params.schema_name }}.activities pa
JOIN {{ params.db_name }}.{{ params.schema_name }}.projects p ON pa.project_id = p.project_id
JOIN {{ params.db_name }}.{{ params.schema_name }}.contractors c ON pa.contractor_id = c.contractor_id
JOIN {{ params.db_name }}.{{ params.schema_name }}.materials m ON pa.material_id = m.material_id;
