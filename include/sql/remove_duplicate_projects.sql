-- Remove duplicate projects based on project_id
-- Keeps the most recent record based on updated_at timestamp

DELETE FROM {{ params.db_name }}.{{ params.schema_name }}.projects 
WHERE project_id IN (
    SELECT project_id 
    FROM (
        SELECT project_id,
               ROW_NUMBER() OVER (
                   PARTITION BY project_id 
                   ORDER BY updated_at DESC
               ) as rn
        FROM {{ params.db_name }}.{{ params.schema_name }}.projects
    ) ranked
    WHERE rn > 1
);
