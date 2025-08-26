-- Remove duplicate materials based on material_id
-- Keeps the most recent record based on updated_at timestamp

DELETE FROM {{ params.db_name }}.{{ params.schema_name }}.materials 
WHERE material_id IN (
    SELECT material_id 
    FROM (
        SELECT material_id,
               ROW_NUMBER() OVER (
                   PARTITION BY material_id 
                   ORDER BY updated_at DESC
               ) as rn
        FROM {{ params.db_name }}.{{ params.schema_name }}.materials
    ) ranked
    WHERE rn > 1
);
