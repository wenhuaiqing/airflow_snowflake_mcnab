-- Upsert project cost analysis data

MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.project_cost_analysis AS target
USING (
    SELECT 
        p.project_id,
        p.project_name,
        p.project_type,
        p.project_status,
        COUNT(pa.activity_id) AS total_activities,
        SUM(pa.total_material_cost) AS total_material_cost,
        SUM(pa.labor_cost) AS total_labor_cost,
        SUM(pa.cost) AS total_cost,
        p.budget AS project_budget,
        CASE 
            WHEN p.budget > 0 THEN (SUM(pa.cost) / p.budget) * 100 
            ELSE 0 
        END AS budget_utilization_percentage,
        CASE 
            WHEN p.square_feet > 0 THEN SUM(pa.cost) / p.square_feet 
            ELSE 0 
        END AS cost_per_sqft,
        AVG(pa.cost) AS avg_activity_cost,
        p.start_date,
        p.estimated_completion,
        p.location,
        p.client_name
    FROM {{ params.db_name }}.{{ params.schema_name }}.projects p
    LEFT JOIN {{ params.db_name }}.{{ params.schema_name }}.enriched_activities pa 
        ON p.project_id = pa.project_id
    GROUP BY p.project_id, p.project_name, p.project_type, p.project_status, 
             p.budget, p.square_feet, p.start_date, p.estimated_completion, 
             p.location, p.client_name
) AS source
ON target.project_id = source.project_id
WHEN MATCHED THEN
    UPDATE SET
        project_name = source.project_name,
        project_type = source.project_type,
        project_status = source.project_status,
        total_activities = source.total_activities,
        total_material_cost = source.total_material_cost,
        total_labor_cost = source.total_labor_cost,
        total_cost = source.total_cost,
        project_budget = source.project_budget,
        budget_utilization_percentage = source.budget_utilization_percentage,
        cost_per_sqft = source.cost_per_sqft,
        avg_activity_cost = source.avg_activity_cost,
        project_start_date = source.start_date,
        estimated_completion = source.estimated_completion,
        location = source.location,
        client_name = source.client_name
WHEN NOT MATCHED THEN
    INSERT (
        project_id, project_name, project_type, project_status, total_activities,
        total_material_cost, total_labor_cost, total_cost, project_budget,
        budget_utilization_percentage, cost_per_sqft, avg_activity_cost,
        project_start_date, estimated_completion, location, client_name
    )
    VALUES (
        source.project_id, source.project_name, source.project_type, source.project_status, source.total_activities,
        source.total_material_cost, source.total_labor_cost, source.total_cost, source.project_budget,
        source.budget_utilization_percentage, source.cost_per_sqft, source.avg_activity_cost,
        source.start_date, source.estimated_completion, source.location, source.client_name
    );
