{{ config(
    alias='bronze_warehouses',
    materialized='table',
    tags=["bronze", "warehouses", "system_table"]
) }}

-- Raw SQL warehouse configuration data from Databricks system tables
-- Contains warehouse setup and configuration details

select 
    account_id,
    warehouse_id,
    created_by,   -- Available column
    change_time,  -- Available column
    delete_time,  -- Available column
    tags,         -- Available column
    -- Note: Using available columns from actual system table
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.compute.warehouses
