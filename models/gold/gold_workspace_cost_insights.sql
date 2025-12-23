{{ config(
    alias='gold_workspace_cost_insights',
    materialized='table',
    tags=["gold", "workspace", "cost", "insights"]
) }}

-- Workspace-level cost analysis and trends for executive reporting
-- Provides high-level cost insights and workspace comparisons

with usage_enriched as (
    select * from {{ ref('silver_usage_enriched') }}
),

workspace_metadata as (
    select * from {{ ref('silver_workspace_metadata') }}
),

-- Monthly workspace cost summary
monthly_workspace_costs as (
    select 
        ue.account_id,
        ue.workspace_id,
        ue.usage_year,
        ue.usage_month,
        
        -- Workspace attributes
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        wm.pricing_tier,
        wm.workspace_age_category,
        wm.workspace_activity_status,
        wm.total_warehouses,
        wm.total_jobs,
        wm.serverless_warehouses,
        
        -- Cost metrics by service category
        sum(case when ue.service_category = 'Jobs Compute' then ue.usage_quantity else 0 end) as jobs_compute_usage,
        sum(case when ue.service_category = 'Interactive Compute' then ue.usage_quantity else 0 end) as interactive_compute_usage,
        sum(case when ue.service_category = 'SQL Warehouse' then ue.usage_quantity else 0 end) as sql_warehouse_usage,
        sum(case when ue.service_category = 'Delta Live Tables' then ue.usage_quantity else 0 end) as dlt_usage,
        sum(case when ue.service_category = 'Serverless' then ue.usage_quantity else 0 end) as serverless_usage,
        sum(case when ue.service_category = 'Storage' then ue.usage_quantity else 0 end) as storage_usage,
        
        -- Total usage and records
        sum(ue.usage_quantity) as total_monthly_usage,
        count(distinct ue.usage_date) as active_days_in_month,
        count(distinct ue.record_id) as total_usage_records,
        
        -- Usage patterns
        avg(ue.usage_quantity) as avg_daily_usage,
        max(ue.usage_quantity) as max_daily_usage,
        stddev(ue.usage_quantity) as usage_volatility,
        
        -- Calculate weekday vs weekend usage
        sum(case when extract(dow from ue.usage_date) in (1, 7) then ue.usage_quantity else 0 end) as weekend_usage,
        sum(case when extract(dow from ue.usage_date) not in (1, 7) then ue.usage_quantity else 0 end) as weekday_usage,
        
        current_timestamp() as gold_processed_at
        
    from usage_enriched ue
    left join workspace_metadata wm
        on ue.workspace_id = wm.workspace_id
        and ue.account_id = wm.account_id
    group by 
        ue.account_id,
        ue.workspace_id,
        ue.usage_year,
        ue.usage_month,
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        wm.pricing_tier,
        wm.workspace_age_category,
        wm.workspace_activity_status,
        wm.total_warehouses,
        wm.total_jobs,
        wm.serverless_warehouses
),

-- Add insights and benchmarking
workspace_insights as (
    select 
        *,
        -- Service mix analysis
        round(jobs_compute_usage / nullif(total_monthly_usage, 0) * 100, 2) as jobs_compute_pct,
        round(interactive_compute_usage / nullif(total_monthly_usage, 0) * 100, 2) as interactive_compute_pct,
        round(sql_warehouse_usage / nullif(total_monthly_usage, 0) * 100, 2) as sql_warehouse_pct,
        round(serverless_usage / nullif(total_monthly_usage, 0) * 100, 2) as serverless_pct,
        
        -- Usage pattern insights
        round(weekend_usage / nullif(total_monthly_usage, 0) * 100, 2) as weekend_usage_pct,
        
        case 
            when weekend_usage / nullif(total_monthly_usage, 0) > 0.3 then '24/7 Operations'
            when weekend_usage / nullif(total_monthly_usage, 0) > 0.1 then 'Some Weekend Activity'
            else 'Business Hours Only'
        end as usage_pattern,
        
        -- Volatility assessment
        case 
            when usage_volatility / nullif(avg_daily_usage, 0) > 1.5 then 'Highly Variable'
            when usage_volatility / nullif(avg_daily_usage, 0) > 0.8 then 'Moderately Variable'
            else 'Stable'
        end as usage_stability,
        
        -- Efficiency indicators
        round(total_monthly_usage / nullif(total_warehouses + total_jobs, 0), 2) as usage_per_resource,
        
        -- Growth trends (compare to previous month)
        lag(total_monthly_usage) over (
            partition by workspace_id 
            order by usage_year, usage_month
        ) as prev_month_usage,
        
        round(
            (total_monthly_usage - lag(total_monthly_usage) over (
                partition by workspace_id 
                order by usage_year, usage_month
            )) / nullif(lag(total_monthly_usage) over (
                partition by workspace_id 
                order by usage_year, usage_month
            ), 0) * 100,
            2
        ) as month_over_month_growth_pct,
        
        -- Rank workspaces by cost within account
        row_number() over (
            partition by account_id, usage_year, usage_month 
            order by total_monthly_usage desc
        ) as cost_rank_in_account,
        
        -- Calculate percentile ranking for benchmarking
        ntile(10) over (
            partition by account_id, usage_year, usage_month 
            order by total_monthly_usage
        ) as cost_decile
        
    from monthly_workspace_costs
),

-- Add final business insights
final_insights as (
    select 
        *,
        -- Business recommendations based on patterns
        case 
            when cost_decile >= 8 and month_over_month_growth_pct > 20 then
                'High-cost workspace with rapid growth - investigate usage drivers'
            when cost_decile >= 7 and usage_stability = 'Highly Variable' then
                'High-cost workspace with volatile usage - optimize resource management'
            when serverless_pct < 20 and total_warehouses > 3 then
                'Consider migrating to serverless for cost optimization'
            when weekend_usage_pct < 5 and total_warehouses > 1 then
                'Implement aggressive auto-stop policies'
            when month_over_month_growth_pct < -20 then
                'Usage declining - consider resource consolidation'
            else 'Monitor usage patterns and optimize as needed'
        end as business_recommendation
        
    from workspace_insights
)

select * from final_insights
order by usage_year desc, usage_month desc, total_monthly_usage desc
