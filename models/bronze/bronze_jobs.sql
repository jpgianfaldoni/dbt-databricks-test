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
    job_name,
    creator_user_name,
    run_as_user_name,
    created_time,
    updated_time,
    job_type,
    schedule,
    max_concurrent_runs,
    timeout_seconds,
    retry_on_timeout,
    max_retries,
    min_retry_interval_millis,
    tags as job_tags,
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.lakeflow.jobs
