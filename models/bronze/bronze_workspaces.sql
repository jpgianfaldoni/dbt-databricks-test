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
    deployment_name,
    workspace_status,
    creation_time,
    workspace_type,
    cloud,
    region,
    is_no_public_ip_enabled,
    storage_configuration,
    network_id,
    storage_customer_managed_key_id,
    managed_services_customer_managed_key_id,
    pricing_tier,
    custom_tags,
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.access.workspaces_latest
