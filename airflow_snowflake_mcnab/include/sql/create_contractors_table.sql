CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.contractors (
    contractor_id STRING PRIMARY KEY,      -- Unique identifier
    contractor_name STRING,                -- Company name
    contractor_type STRING,                -- General, Electrical, etc.
    license_number STRING,                 -- License ID
    license_type STRING,                   -- Class A, B, C, etc.
    years_experience INT,                  -- Years in business
    phone STRING,                          -- Contact phone
    email STRING,                          -- Contact email
    address STRING,                        -- Business address
    registration_date DATE,                -- When registered
    updated_at TIMESTAMP                   -- Last update timestamp
);