-- Remove duplicate project activities based on activity_id
-- Keeps the most recent record based on updated_at timestamp

DELETE FROM {{ params.db_name }}.{{ params.schema_name }}.activities 
WHERE activity_id IN (
    SELECT activity_id 
    FROM (
        SELECT activity_id,
               ROW_NUMBER() OVER (
                   PARTITION BY activity_id 
                   ORDER BY updated_at DESC
               ) as rn
        FROM {{ params.db_name }}.{{ params.schema_name }}.activities
    ) ranked
    WHERE rn > 1
);
