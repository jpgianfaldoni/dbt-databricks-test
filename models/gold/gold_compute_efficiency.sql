{{ config(
    alias='gold_compute_efficiency',
    materialized='table',
    tags=["gold", "compute", "efficiency", "analytics"]
) }}

-- Compute utilization efficiency metrics for resource optimization
-- Identifies underutilized resources and optimization opportunities

with compute_metrics as (
    select * from {{ ref('silver_compute_metrics') }}
),

workspace_metadata as (
    select * from {{ ref('silver_workspace_metadata') }}
),

-- Daily efficiency rollups per cluster
daily_efficiency as (
    select 
        cm.account_id,
        cm.workspace_id,
        cm.cluster_id,
        cm.node_type,
        date(cm.metric_hour) as efficiency_date,
        
        -- Workspace context
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        
        -- Daily utilization metrics
        count(*) as hourly_samples,
        avg(cm.avg_cpu_percent) as daily_avg_cpu,
        max(cm.max_cpu_percent) as daily_max_cpu,
        avg(cm.avg_memory_percent) as daily_avg_memory,
        max(cm.max_memory_percent) as daily_max_memory,
        avg(cm.overall_utilization_score) as daily_efficiency_score,
        
        -- Peak vs off-peak analysis (business hours: 8-18)
        avg(case 
            when cm.metric_hour_of_day between 8 and 18 then cm.overall_utilization_score 
        end) as business_hours_efficiency,
        
        avg(case 
            when cm.metric_hour_of_day not between 8 and 18 then cm.overall_utilization_score 
        end) as off_hours_efficiency,
        
        -- Resource waste indicators
        sum(case 
            when cm.overall_utilization_score < 20 then 1 else 0 
        end) as low_utilization_hours,
        
        sum(case 
            when cm.overall_utilization_score > 80 then 1 else 0 
        end) as high_utilization_hours,
        
        -- Efficiency classification
        case 
            when avg(cm.overall_utilization_score) >= 70 then 'Highly Efficient'
            when avg(cm.overall_utilization_score) >= 50 then 'Moderately Efficient'
            when avg(cm.overall_utilization_score) >= 30 then 'Inefficient'
            else 'Highly Inefficient'
        end as efficiency_category,
        
        current_timestamp() as gold_processed_at
        
    from compute_metrics cm
    left join workspace_metadata wm
        on cm.workspace_id = wm.workspace_id
        and cm.account_id = wm.account_id
    group by 
        cm.account_id,
        cm.workspace_id,
        cm.cluster_id,
        cm.node_type,
        date(cm.metric_hour),
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region
),

-- Add optimization recommendations
efficiency_with_recommendations as (
    select 
        *,
        -- Calculate potential savings opportunities
        case 
            when daily_efficiency_score < 30 and low_utilization_hours > 12 then
                'Consider downsizing cluster or implementing auto-scaling'
            when business_hours_efficiency > 70 and off_hours_efficiency < 30 then
                'Implement aggressive auto-stop policies for off-hours'
            when daily_max_cpu < 50 and daily_max_memory < 50 then
                'Cluster appears oversized - consider smaller node types'
            when daily_efficiency_score between 30 and 50 then
                'Monitor usage patterns and consider right-sizing'
            else 'Cluster utilization appears optimal'
        end as optimization_recommendation,
        
        -- Calculate waste score (0-100, higher = more waste)
        round(
            case 
                when daily_efficiency_score < 20 then 80 + (20 - daily_efficiency_score)
                when daily_efficiency_score < 50 then 50 + (50 - daily_efficiency_score) * 0.6
                else greatest(0, 30 - daily_efficiency_score * 0.6)
            end,
            2
        ) as waste_score,
        
        -- Add trend analysis (7-day comparison)
        avg(daily_efficiency_score) over (
            partition by cluster_id 
            order by efficiency_date 
            rows between 6 preceding and current row
        ) as efficiency_7day_avg,
        
        -- Previous day efficiency for comparison
        lag(daily_efficiency_score) over (
            partition by cluster_id 
            order by efficiency_date
        ) as prev_day_efficiency
        
    from daily_efficiency
)

select * from efficiency_with_recommendations
order by efficiency_date desc, waste_score desc
