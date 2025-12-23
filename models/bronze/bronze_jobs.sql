{{ config(
    alias='bronze_jobs',
    materialized='table',
    tags=["bronze", "jobs", "system_table"]
) }}

-- Raw job configuration data from Databricks system tables
-- Contains job setup and execution information

select 
    account_id,
    workspace_id,
    job_id,
    name,    -- Corrected from job_name
    run_as,  -- Corrected from run_as_user_name
    paused,  -- Available column
    tags,    -- Available column
    -- Note: Using available columns from actual system table
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.lakeflow.jobs
