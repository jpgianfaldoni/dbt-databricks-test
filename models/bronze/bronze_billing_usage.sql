{{ config(
    alias='bronze_billing_usage',
    materialized='table',
    tags=["bronze", "billing", "system_table"]
) }}

-- Raw billing usage data from Databricks system tables
-- This model ingests all billable usage events with minimal transformation

select 
    account_id,
    workspace_id,
    sku_name,
    cloud,
    custom_tags,
    usage_start_time,
    usage_end_time,
    usage_date,
    usage_unit,
    usage_quantity,
    record_id,
    ingestion_date,  -- Corrected from ingestion_time
    usage_metadata,
    -- Add processing timestamp for audit trail
    current_timestamp() as bronze_processed_at
from system.billing.usage

-- For incremental processing in production
{% if is_incremental() %}
    where usage_date >= current_date() - interval '7' day
{% endif %}
