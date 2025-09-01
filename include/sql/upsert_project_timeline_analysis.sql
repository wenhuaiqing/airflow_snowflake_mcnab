-- Upsert project timeline analysis data

MERGE INTO {{ params.db_name }}.{{ params.schema_name }}.project_timeline_analysis AS target
USING (
    SELECT 
        p.project_id,
        p.project_name,
        p.project_type,
        p.project_status,
        p.start_date,
        p.estimated_completion,
        CASE 
            WHEN p.project_status = 'Completed' THEN p.estimated_completion
            ELSE NULL 
        END AS actual_completion_date,
        CASE 
            WHEN p.project_status = 'Completed' THEN 
                DATEDIFF('day', p.start_date, p.estimated_completion)
            ELSE 
                DATEDIFF('day', p.start_date, CURRENT_DATE())
        END AS project_duration_days,
        DATEDIFF('day', p.start_date, p.estimated_completion) AS estimated_duration_days,
        CASE 
            WHEN p.project_status = 'Completed' THEN 
                DATEDIFF('day', p.start_date, p.estimated_completion) - DATEDIFF('day', p.start_date, p.estimated_completion)
            ELSE 
                DATEDIFF('day', CURRENT_DATE(), p.estimated_completion)
        END AS timeline_variance_days,
        CASE 
            WHEN DATEDIFF('day', p.start_date, p.estimated_completion) > 0 THEN
                (DATEDIFF('day', CURRENT_DATE(), p.estimated_completion) / DATEDIFF('day', p.start_date, p.estimated_completion)) * 100
            ELSE 0 
        END AS timeline_variance_percentage,
        COUNT(pa.activity_id) AS activities_completed,
        COUNT(pa.activity_id) AS total_activities,
        CASE 
            WHEN COUNT(pa.activity_id) > 0 THEN 100
            ELSE 0 
        END AS completion_percentage,
        CASE 
            WHEN DATEDIFF('day', p.start_date, CURRENT_DATE()) > 0 THEN
                COUNT(pa.activity_id) / DATEDIFF('day', p.start_date, CURRENT_DATE())
            ELSE 0 
        END AS avg_activities_per_day,
        MAX(pa.activity_date) AS last_activity_date,
        p.location,
        p.client_name
    FROM {{ params.db_name }}.{{ params.schema_name }}.projects p
    LEFT JOIN {{ params.db_name }}.{{ params.schema_name }}.enriched_activities pa 
        ON p.project_id = pa.project_id
    GROUP BY p.project_id, p.project_name, p.project_type, p.project_status, 
             p.start_date, p.estimated_completion, p.location, p.client_name
) AS source
ON target.project_id = source.project_id
WHEN MATCHED THEN
    UPDATE SET
        project_name = source.project_name,
        project_type = source.project_type,
        project_status = source.project_status,
        start_date = source.start_date,
        estimated_completion = source.estimated_completion,
        actual_completion_date = source.actual_completion_date,
        project_duration_days = source.project_duration_days,
        estimated_duration_days = source.estimated_duration_days,
        timeline_variance_days = source.timeline_variance_days,
        timeline_variance_percentage = source.timeline_variance_percentage,
        activities_completed = source.activities_completed,
        total_activities = source.total_activities,
        completion_percentage = source.completion_percentage,
        avg_activities_per_day = source.avg_activities_per_day,
        last_activity_date = source.last_activity_date,
        location = source.location,
        client_name = source.client_name
WHEN NOT MATCHED THEN
    INSERT (
        project_id, project_name, project_type, project_status, start_date,
        estimated_completion, actual_completion_date, project_duration_days, estimated_duration_days,
        timeline_variance_days, timeline_variance_percentage, activities_completed, total_activities,
        completion_percentage, avg_activities_per_day, last_activity_date, location, client_name
    )
    VALUES (
        source.project_id, source.project_name, source.project_type, source.project_status, source.start_date,
        source.estimated_completion, source.actual_completion_date, source.project_duration_days, source.estimated_duration_days,
        source.timeline_variance_days, source.timeline_variance_percentage, source.activities_completed, source.total_activities,
        source.completion_percentage, source.avg_activities_per_day, source.last_activity_date, source.location, source.client_name
    );
