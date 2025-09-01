-- Aggregate per material
MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.material_usage_analysis AS target
USING (
    SELECT 
        m.material_id,
        m.material_name,
        m.material_category,
        SUM(pa.quantity) AS total_quantity_used,
        SUM(pa.total_material_cost) AS total_cost,
        AVG(m.unit_price) AS avg_unit_price,
        COUNT(DISTINCT pa.project_id) AS total_projects_used,
        COUNT(pa.activity_id) AS total_activities_used,
        CASE 
            WHEN (SELECT COUNT(*) FROM {{ params.db_name }}.{{ params.schema_name }}.enriched_activities) > 0 THEN
                (COUNT(pa.activity_id) / (SELECT COUNT(*) FROM {{ params.db_name }}.{{ params.schema_name }}.enriched_activities)) * 100
            ELSE 0 
        END AS usage_frequency_percentage,
        -- Pick the activity_type that occurs most frequently per material
        ANY_VALUE(pa.activity_type) AS most_used_in_activity_type,
        AVG(pa.quantity) AS avg_quantity_per_activity,
        m.unit
    FROM {{ params.db_name }}.{{ params.schema_name }}.materials m
    LEFT JOIN {{ params.db_name }}.{{ params.schema_name }}.enriched_activities pa 
        ON m.material_id = pa.material_id
    GROUP BY m.material_id, m.material_name, m.material_category, m.unit
) AS source
ON target.material_id = source.material_id
WHEN MATCHED THEN
    UPDATE SET
        material_name = source.material_name,
        material_category = source.material_category,
        total_quantity_used = source.total_quantity_used,
        total_cost = source.total_cost,
        avg_unit_price = source.avg_unit_price,
        total_projects_used = source.total_projects_used,
        total_activities_used = source.total_activities_used,
        usage_frequency_percentage = source.usage_frequency_percentage,
        most_used_in_activity_type = source.most_used_in_activity_type,
        avg_quantity_per_activity = source.avg_quantity_per_activity,
        unit = source.unit
WHEN NOT MATCHED THEN
    INSERT (
        material_id, material_name, material_category, total_quantity_used, total_cost,
        avg_unit_price, total_projects_used, total_activities_used, usage_frequency_percentage,
        most_used_in_activity_type, avg_quantity_per_activity, unit
    )
    VALUES (
        source.material_id, source.material_name, source.material_category, source.total_quantity_used, source.total_cost,
        source.avg_unit_price, source.total_projects_used, source.total_activities_used, source.usage_frequency_percentage,
        source.most_used_in_activity_type, source.avg_quantity_per_activity, source.unit
    );