{{ config(
    alias='silver_usage_summary',
    materialized='table',
    tags=["silver", "usage", "summary"]
) }}

-- Cleaned and summarized billing usage data
-- Aggregates usage by workspace and service category with basic enrichment

with billing_usage as (
    select * from {{ ref('bronze_billing_usage') }}
),

usage_with_categories as (
    select 
        account_id,
        workspace_id,
        usage_date,
        sku_name,
        cloud,
        usage_unit,
        usage_quantity,
        
        -- Categorize SKUs for better analysis
        case 
            when sku_name like '%JOBS%' then 'Jobs Compute'
            when sku_name like '%ALL_PURPOSE%' then 'Interactive Compute'
            when sku_name like '%SQL%' then 'SQL Warehouse'
            when sku_name like '%DLT%' then 'Delta Live Tables'
            when sku_name like '%SERVERLESS%' then 'Serverless'
            when sku_name like '%STORAGE%' then 'Storage'
            else 'Other'
        end as service_category,
        
        -- Extract compute type from SKU
        case 
            when sku_name like '%PHOTON%' then 'Photon'
            when sku_name like '%STANDARD%' then 'Standard'
            when sku_name like '%PREMIUM%' then 'Premium'
            else 'Unknown'
        end as compute_type,
        
        -- Add date dimensions for time-series analysis
        extract(year from usage_date) as usage_year,
        extract(month from usage_date) as usage_month,
        extract(day from usage_date) as usage_day,
        extract(dow from usage_date) as usage_day_of_week,
        
        -- Add weekday/weekend classification
        case 
            when extract(dow from usage_date) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as day_type,
        
        current_timestamp() as silver_processed_at
        
    from billing_usage
)

select * from usage_with_categories
