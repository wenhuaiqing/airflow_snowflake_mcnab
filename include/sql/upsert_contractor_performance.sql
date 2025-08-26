-- Upsert contractor performance data

MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.contractor_performance AS target
USING (
    SELECT 
        c.contractor_id,
        c.contractor_name,
        c.contractor_type,
        COUNT(DISTINCT pa.project_id) AS total_projects,
        COUNT(pa.activity_id) AS total_activities,
        SUM(pa.hours_worked) AS total_hours_worked,
        SUM(pa.cost) AS total_cost,
        CASE 
            WHEN SUM(pa.hours_worked) > 0 THEN SUM(pa.cost) / SUM(pa.hours_worked)
            ELSE 0 
        END AS avg_cost_per_hour,
        AVG(pa.cost) AS avg_activity_cost,
        COUNT(DISTINCT CASE WHEN p.project_status = 'Completed' THEN pa.project_id END) AS projects_completed,
        COUNT(DISTINCT CASE WHEN p.project_status = 'In Progress' THEN pa.project_id END) AS projects_in_progress,
        CASE 
            WHEN COUNT(DISTINCT pa.project_id) > 0 THEN 
                (COUNT(DISTINCT CASE WHEN p.project_status = 'Completed' THEN pa.project_id END) / COUNT(DISTINCT pa.project_id)) * 100
            ELSE 0 
        END AS completion_rate_percentage,
        c.years_experience,
        c.license_type,
        c.registration_date
    FROM {{ params.db_name }}.{{ params.schema_name }}.contractors c
    LEFT JOIN {{ params.db_name }}.{{ params.schema_name }}.enriched_project_activities pa 
        ON c.contractor_id = pa.contractor_id
    LEFT JOIN {{ params.db_name }}.{{ params.schema_name }}.projects p 
        ON pa.project_id = p.project_id
    GROUP BY c.contractor_id, c.contractor_name, c.contractor_type, c.years_experience, 
             c.license_type, c.registration_date
) AS source
ON target.contractor_id = source.contractor_id
WHEN MATCHED THEN
    UPDATE SET
        contractor_name = source.contractor_name,
        contractor_type = source.contractor_type,
        total_projects = source.total_projects,
        total_activities = source.total_activities,
        total_hours_worked = source.total_hours_worked,
        total_cost = source.total_cost,
        avg_cost_per_hour = source.avg_cost_per_hour,
        avg_activity_cost = source.avg_activity_cost,
        projects_completed = source.projects_completed,
        projects_in_progress = source.projects_in_progress,
        completion_rate_percentage = source.completion_rate_percentage,
        years_experience = source.years_experience,
        license_type = source.license_type,
        registration_date = source.registration_date
WHEN NOT MATCHED THEN
    INSERT (
        contractor_id, contractor_name, contractor_type, total_projects, total_activities,
        total_hours_worked, total_cost, avg_cost_per_hour, avg_activity_cost,
        projects_completed, projects_in_progress, completion_rate_percentage,
        years_experience, license_type, registration_date
    )
    VALUES (
        source.contractor_id, source.contractor_name, source.contractor_type, source.total_projects, source.total_activities,
        source.total_hours_worked, source.total_cost, source.avg_cost_per_hour, source.avg_activity_cost,
        source.projects_completed, source.projects_in_progress, source.completion_rate_percentage,
        source.years_experience, source.license_type, source.registration_date
    );
