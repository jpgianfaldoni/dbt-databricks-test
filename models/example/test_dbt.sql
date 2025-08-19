{{ config(
    alias='dbt_test',
    materialized='table'
) }}

select 
    workspace_id,
    sum(usage_quantity) as total_usage_quantity
from system.billing.usage
group by workspace_id
