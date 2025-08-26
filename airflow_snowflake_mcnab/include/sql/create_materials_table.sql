CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.materials (
    material_id STRING PRIMARY KEY,        -- Unique identifier
    material_name STRING,                  -- Concrete, Steel, etc.
    material_category STRING,              -- Foundation, Structural, etc.
    unit_price NUMBER,                     -- Price per unit
    unit STRING,                           -- piece, sqft, linear_ft, etc.
    updated_at TIMESTAMP                   -- Last update timestamp
);