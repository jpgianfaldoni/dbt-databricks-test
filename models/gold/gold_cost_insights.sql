{{ config(
    alias='gold_cost_insights',
    materialized='table',
    tags=["gold", "cost", "insights", "analytics"]
) }}

-- Business-ready cost insights and analytics
-- Daily cost summaries with trends and workspace comparisons

with usage_summary as (
    select * from {{ ref('silver_usage_summary') }}
),

daily_cost_summary as (
    select 
        account_id,
        workspace_id,
        usage_date,
        usage_year,
        usage_month,
        usage_day,
        day_type,
        
        -- Service breakdown
        sum(case when service_category = 'Jobs Compute' then usage_quantity else 0 end) as jobs_compute_usage,
        sum(case when service_category = 'Interactive Compute' then usage_quantity else 0 end) as interactive_compute_usage,
        sum(case when service_category = 'SQL Warehouse' then usage_quantity else 0 end) as sql_warehouse_usage,
        sum(case when service_category = 'Serverless' then usage_quantity else 0 end) as serverless_usage,
        sum(case when service_category = 'Storage' then usage_quantity else 0 end) as storage_usage,
        sum(case when service_category = 'Other' then usage_quantity else 0 end) as other_usage,
        
        -- Total usage metrics
        sum(usage_quantity) as total_daily_usage,
        count(distinct sku_name) as unique_skus_used,
        
        current_timestamp() as gold_processed_at
        
    from usage_summary
    group by 
        account_id,
        workspace_id,
        usage_date,
        usage_year,
        usage_month,
        usage_day,
        day_type
),

cost_insights_with_trends as (
    select 
        *,
        -- Calculate service mix percentages
        round(jobs_compute_usage / nullif(total_daily_usage, 0) * 100, 2) as jobs_compute_pct,
        round(interactive_compute_usage / nullif(total_daily_usage, 0) * 100, 2) as interactive_compute_pct,
        round(sql_warehouse_usage / nullif(total_daily_usage, 0) * 100, 2) as sql_warehouse_pct,
        round(serverless_usage / nullif(total_daily_usage, 0) * 100, 2) as serverless_pct,
        
        -- 7-day moving average
        avg(total_daily_usage) over (
            partition by workspace_id 
            order by usage_date 
            rows between 6 preceding and current row
        ) as usage_7day_avg,
        
        -- Previous day comparison
        lag(total_daily_usage) over (
            partition by workspace_id 
            order by usage_date
        ) as prev_day_usage,
        
        -- Day-over-day change percentage
        round(
            (total_daily_usage - lag(total_daily_usage) over (
                partition by workspace_id 
                order by usage_date
            )) / nullif(lag(total_daily_usage) over (
                partition by workspace_id 
                order by usage_date
            ), 0) * 100,
            2
        ) as daily_change_pct,
        
        -- Rank workspaces by daily usage
        row_number() over (
            partition by account_id, usage_date 
            order by total_daily_usage desc
        ) as daily_usage_rank
        
    from daily_cost_summary
)

select * from cost_insights_with_trends
order by usage_date desc, total_daily_usage desc
