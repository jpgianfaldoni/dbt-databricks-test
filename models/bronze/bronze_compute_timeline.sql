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
    timestamp,
    cpu_percent,
    memory_percent,
    available_disk_bytes,
    disk_io_throughput_bytes,
    network_io_throughput_bytes,
    ingestion_time,
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from {{ source('system', 'compute_node_timeline') }}

-- For incremental processing in production
{% if is_incremental() %}
    where date(timestamp) >= current_date() - interval '7' day
{% endif %}
