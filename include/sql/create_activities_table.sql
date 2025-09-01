CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.activities (
    activity_id STRING PRIMARY KEY,        -- Unique identifier
    project_id STRING,                     -- Links to projects table
    contractor_id STRING,                  -- Links to contractors table
    material_id STRING,                    -- Links to materials table
    activity_type STRING,                  -- Foundation, Framing, etc.
    quantity INT,                          -- Amount of work/materials
    activity_date TIMESTAMP,               -- When activity occurred
    hours_worked INT,                      -- Labor hours
    cost NUMBER,                           -- Activity cost
    updated_at TIMESTAMP                   -- Last update timestamp
);