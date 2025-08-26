-- Remove duplicate contractors based on contractor_id
-- Keeps the most recent record based on updated_at timestamp

DELETE FROM {{ params.db_name }}.{{ params.schema_name }}.contractors 
WHERE contractor_id IN (
    SELECT contractor_id 
    FROM (
        SELECT contractor_id,
               ROW_NUMBER() OVER (
                   PARTITION BY contractor_id 
                   ORDER BY updated_at DESC
               ) as rn
        FROM {{ params.db_name }}.{{ params.schema_name }}.contractors
    ) ranked
    WHERE rn > 1
);
