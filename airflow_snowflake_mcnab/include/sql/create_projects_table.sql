CREATE TABLE IF NOT EXISTS {{ params.db_name }}.{{ params.schema_name }}.projects (
    project_id STRING PRIMARY KEY,         -- Unique identifier
    project_name STRING,                   -- Project name
    project_type STRING,                   -- Residential, Commercial, etc.
    project_status STRING,                 -- Planning, In Progress, etc.
    start_date DATE,                       -- Project start date
    estimated_completion DATE,             -- Expected completion
    budget NUMBER,                         -- Total project budget
    square_feet INT,                       -- Project size
    location STRING,                       -- Project location
    client_name STRING,                    -- Client company
    architect STRING,                      -- Architect name
    updated_at TIMESTAMP                   -- Last update timestamp
);