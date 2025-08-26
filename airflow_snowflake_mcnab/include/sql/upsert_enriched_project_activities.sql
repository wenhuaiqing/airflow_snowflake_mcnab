-- Upsert enriched project activities data
-- This script handles incremental updates to the enriched table

MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.enriched_project_activities AS target
USING (
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
    FROM {{ params.db_name }}.{{ params.schema_name }}.project_activities pa
    JOIN {{ params.db_name }}.{{ params.schema_name }}.projects p ON pa.project_id = p.project_id
    JOIN {{ params.db_name }}.{{ params.schema_name }}.contractors c ON pa.contractor_id = c.contractor_id
    JOIN {{ params.db_name }}.{{ params.schema_name }}.materials m ON pa.material_id = m.material_id
) AS source
ON target.activity_id = source.activity_id
WHEN MATCHED THEN
    UPDATE SET
        project_name = source.project_name,
        project_type = source.project_type,
        project_status = source.project_status,
        contractor_name = source.contractor_name,
        contractor_type = source.contractor_type,
        material_name = source.material_name,
        material_category = source.material_category,
        activity_type = source.activity_type,
        quantity = source.quantity,
        unit_price = source.unit_price,
        unit = source.unit,
        activity_date = source.activity_date,
        hours_worked = source.hours_worked,
        cost = source.cost,
        total_material_cost = source.total_material_cost,
        labor_cost = source.labor_cost,
        project_budget = source.project_budget,
        project_location = source.project_location,
        client_name = source.client_name,
        architect = source.architect
WHEN NOT MATCHED THEN
    INSERT (
        activity_id, project_id, project_name, project_type, project_status,
        contractor_id, contractor_name, contractor_type, material_id, material_name,
        material_category, activity_type, quantity, unit_price, unit,
        activity_date, hours_worked, cost, total_material_cost, labor_cost,
        project_budget, project_location, client_name, architect
    )
    VALUES (
        source.activity_id, source.project_id, source.project_name, source.project_type, source.project_status,
        source.contractor_id, source.contractor_name, source.contractor_type, source.material_id, source.material_name,
        source.material_category, source.activity_type, source.quantity, source.unit_price, source.unit,
        source.activity_date, source.hours_worked, source.cost, source.total_material_cost, source.labor_cost,
        source.project_budget, source.project_location, source.client_name, source.architect
    );
