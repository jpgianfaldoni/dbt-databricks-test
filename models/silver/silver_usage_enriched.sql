{{ config(
    alias='silver_usage_enriched',
    materialized='table',
    tags=["silver", "usage", "enriched"]
) }}

-- Enriched billing usage data with workspace and compute context
-- Joins billing data with workspace metadata for enhanced analytics

with billing_usage as (
    select * from {{ ref('bronze_billing_usage') }}
),

workspace_metadata as (
    select * from {{ ref('bronze_workspaces') }}
),

-- Create cost calculations and categorizations
usage_enriched as (
    select 
        bu.account_id,
        bu.workspace_id,
        wm.workspace_name,
        wm.deployment_name,
        wm.workspace_type,
        wm.cloud,
        wm.region,
        wm.pricing_tier,
        bu.sku_name,
        bu.usage_start_time,
        bu.usage_end_time,
        bu.usage_date,
        bu.usage_unit,
        bu.usage_quantity,
        
        -- Categorize SKUs for better analysis
        case 
            when bu.sku_name like '%JOBS%' then 'Jobs Compute'
            when bu.sku_name like '%ALL_PURPOSE%' then 'Interactive Compute'
            when bu.sku_name like '%SQL%' then 'SQL Warehouse'
            when bu.sku_name like '%DLT%' then 'Delta Live Tables'
            when bu.sku_name like '%SERVERLESS%' then 'Serverless'
            when bu.sku_name like '%STORAGE%' then 'Storage'
            else 'Other'
        end as service_category,
        
        -- Extract compute type from SKU for detailed analysis
        case 
            when bu.sku_name like '%PHOTON%' then 'Photon'
            when bu.sku_name like '%STANDARD%' then 'Standard'
            when bu.sku_name like '%PREMIUM%' then 'Premium'
            else 'Unknown'
        end as compute_type,
        
        -- Calculate usage duration in hours for compute services
        case 
            when bu.usage_unit = 'DBU' then 
                extract(epoch from bu.usage_end_time - bu.usage_start_time) / 3600.0
            else null
        end as usage_duration_hours,
        
        -- Add date dimensions for time-series analysis
        extract(year from bu.usage_date) as usage_year,
        extract(month from bu.usage_date) as usage_month,
        extract(day from bu.usage_date) as usage_day,
        extract(dow from bu.usage_date) as usage_day_of_week,
        
        bu.custom_tags,
        bu.record_id,
        current_timestamp() as silver_processed_at
        
    from billing_usage bu
    left join workspace_metadata wm 
        on bu.workspace_id = wm.workspace_id 
        and bu.account_id = wm.account_id
)

select * from usage_enriched
