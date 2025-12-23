{{ config(
    alias='bronze_warehouses',
    materialized='table',
    tags=["bronze", "warehouses", "system_table"]
) }}

-- Raw SQL warehouse configuration data from Databricks system tables
-- Contains warehouse setup and configuration details

select 
    account_id,
    workspace_id,
    warehouse_id,
    name as warehouse_name,
    size,
    cluster_size,
    min_num_clusters,
    max_num_clusters,
    auto_stop_mins,
    auto_resume,
    warehouse_type,
    state,
    creator_id,
    creator_name,
    creation_time,
    deleted_time,
    updated_time,
    enable_photon,
    enable_serverless_compute,
    spot_instance_policy,
    channel,
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.compute.warehouses
