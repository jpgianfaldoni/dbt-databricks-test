{{ config(
    alias='bronze_compute_timeline',
    materialized='table',
    tags=["bronze", "compute", "system_table"]
) }}

-- Raw compute node timeline data from Databricks system tables
-- Captures utilization metrics for all compute resources

select 
    account_id,
    workspace_id,
    cluster_id,
    node_type,
    start_time,  -- Corrected from timestamp
    end_time,    -- Added end_time
    driver,      -- Added driver  
    cpu_percent,
    memory_percent,
    available_disk_bytes,
    disk_io_throughput_bytes,
    network_io_throughput_bytes,
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.compute.node_timeline

-- For incremental processing in production
{% if is_incremental() %}
    where date(start_time) >= current_date() - interval '7' day
{% endif %}
