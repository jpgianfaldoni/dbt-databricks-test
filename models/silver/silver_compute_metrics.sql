{{ config(
    alias='silver_compute_metrics',
    materialized='table',
    tags=["silver", "compute", "metrics"]
) }}

-- Aggregated compute utilization metrics with performance calculations
-- Provides hourly rollups of compute usage across all clusters

with compute_timeline as (
    select * from {{ ref('bronze_compute_timeline') }}
),

workspace_metadata as (
    select * from {{ ref('bronze_workspaces') }}
),

-- Calculate hourly aggregated metrics per cluster
hourly_metrics as (
    select 
        ct.account_id,
        ct.workspace_id,
        ct.cluster_id,
        ct.node_type,
        date_trunc('hour', ct.start_time) as metric_hour,
        
        -- CPU utilization metrics
        avg(ct.cpu_percent) as avg_cpu_percent,
        max(ct.cpu_percent) as max_cpu_percent,
        min(ct.cpu_percent) as min_cpu_percent,
        
        -- Memory utilization metrics
        avg(ct.memory_percent) as avg_memory_percent,
        max(ct.memory_percent) as max_memory_percent,
        min(ct.memory_percent) as min_memory_percent,
        
        -- Disk and network I/O metrics
        avg(ct.available_disk_bytes) as avg_available_disk_bytes,
        avg(ct.disk_io_throughput_bytes) as avg_disk_io_throughput_mb,
        avg(ct.network_io_throughput_bytes) as avg_network_io_throughput_mb,
        
        -- Calculate efficiency scores
        case 
            when avg(ct.cpu_percent) > 80 then 'High'
            when avg(ct.cpu_percent) > 50 then 'Medium'
            else 'Low'
        end as cpu_utilization_category,
        
        case 
            when avg(ct.memory_percent) > 80 then 'High'
            when avg(ct.memory_percent) > 50 then 'Medium'
            else 'Low'
        end as memory_utilization_category,
        
        -- Count of data points for quality assessment
        count(*) as sample_count,
        
        current_timestamp() as silver_processed_at
        
    from compute_timeline ct
    group by 
        ct.account_id,
        ct.workspace_id, 
        ct.cluster_id,
        ct.node_type,
        date_trunc('hour', ct.start_time)
),

-- Enrich with workspace context
metrics_enriched as (
    select 
        hm.*,
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        
        -- Add date dimensions for analysis
        extract(year from hm.metric_hour) as metric_year,
        extract(month from hm.metric_hour) as metric_month,
        extract(day from hm.metric_hour) as metric_day,
        extract(hour from hm.metric_hour) as metric_hour_of_day,
        extract(dow from hm.metric_hour) as metric_day_of_week,
        
        -- Calculate overall efficiency score (0-100)
        round(
            (hm.avg_cpu_percent + hm.avg_memory_percent) / 2.0, 2
        ) as overall_utilization_score
        
    from hourly_metrics hm
    left join workspace_metadata wm 
        on hm.workspace_id = wm.workspace_id 
        and hm.account_id = wm.account_id
)

select * from metrics_enriched
