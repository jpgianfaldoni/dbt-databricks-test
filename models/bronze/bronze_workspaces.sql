{{ config(
    alias='bronze_workspaces',
    materialized='table',
    tags=["bronze", "workspaces", "system_table"]
) }}

-- Raw workspace metadata from Databricks system tables
-- Contains workspace configuration and metadata information

select 
    account_id,
    workspace_id,
    workspace_name,
    status,      -- Corrected from workspace_status
    create_time, -- Corrected from creation_time
    -- Note: Many columns don't exist in actual system table, using available ones
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.access.workspaces_latest
