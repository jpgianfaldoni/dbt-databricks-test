{{ config(
    alias='gold_cost_summary_daily',
    materialized='table',
    tags=["gold", "cost", "daily", "analytics"]
) }}

-- Daily cost breakdowns by workspace and service category
-- Provides comprehensive daily spend analysis for cost optimization

with usage_enriched as (
    select * from {{ ref('silver_usage_enriched') }}
),

workspace_metadata as (
    select * from {{ ref('silver_workspace_metadata') }}
),

-- Daily cost aggregations
daily_costs as (
    select 
        ue.account_id,
        ue.workspace_id,
        ue.usage_date,
        ue.service_category,
        ue.compute_type,
        ue.sku_name,
        
        -- Workspace context
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        wm.pricing_tier,
        wm.workspace_age_category,
        wm.network_security_type,
        
        -- Usage metrics
        count(distinct ue.record_id) as usage_records,
        sum(ue.usage_quantity) as total_usage_quantity,
        avg(ue.usage_quantity) as avg_usage_quantity,
        sum(coalesce(ue.usage_duration_hours, 0)) as total_usage_hours,
        
        -- Time dimensions
        ue.usage_year,
        ue.usage_month,
        ue.usage_day,
        ue.usage_day_of_week,
        
        -- Add weekday/weekend classification
        case 
            when ue.usage_day_of_week in (1, 7) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        current_timestamp() as gold_processed_at
        
    from usage_enriched ue
    left join workspace_metadata wm
        on ue.workspace_id = wm.workspace_id
        and ue.account_id = wm.account_id
    group by 
        ue.account_id,
        ue.workspace_id,
        ue.usage_date,
        ue.service_category,
        ue.compute_type,
        ue.sku_name,
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        wm.pricing_tier,
        wm.workspace_age_category,
        wm.network_security_type,
        ue.usage_year,
        ue.usage_month,
        ue.usage_day,
        ue.usage_day_of_week
),

-- Add moving averages and trends
cost_with_trends as (
    select 
        *,
        -- 7-day moving average of usage
        avg(total_usage_quantity) over (
            partition by workspace_id, service_category 
            order by usage_date 
            rows between 6 preceding and current row
        ) as usage_7day_avg,
        
        -- Month-to-date usage
        sum(total_usage_quantity) over (
            partition by workspace_id, service_category, usage_year, usage_month
            order by usage_date
        ) as usage_month_to_date,
        
        -- Previous day comparison
        lag(total_usage_quantity) over (
            partition by workspace_id, service_category 
            order by usage_date
        ) as prev_day_usage,
        
        -- Calculate day-over-day change percentage
        round(
            (total_usage_quantity - lag(total_usage_quantity) over (
                partition by workspace_id, service_category 
                order by usage_date
            )) / nullif(lag(total_usage_quantity) over (
                partition by workspace_id, service_category 
                order by usage_date
            ), 0) * 100,
            2
        ) as usage_change_pct
        
    from daily_costs
)

select * from cost_with_trends
order by usage_date desc, workspace_name, service_category
