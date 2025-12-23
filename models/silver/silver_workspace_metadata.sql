{{ config(
    alias='silver_workspace_metadata',
    materialized='table',
    tags=["silver", "workspace", "metadata"]
) }}

-- Enhanced workspace information with cost allocation and configuration details
-- Combines workspace, warehouse, and job metadata for comprehensive workspace view

with workspaces as (
    select * from {{ ref('bronze_workspaces') }}
),

warehouses as (
    select * from {{ ref('bronze_warehouses') }}
),

jobs as (
    select * from {{ ref('bronze_jobs') }}
),

-- Aggregate warehouse information per workspace
warehouse_summary as (
    select 
        account_id,
        workspace_id,
        count(*) as total_warehouses,
        count(case when state = 'RUNNING' then 1 end) as running_warehouses,
        count(case when enable_serverless_compute then 1 end) as serverless_warehouses,
        count(case when enable_photon then 1 end) as photon_enabled_warehouses,
        string_agg(distinct size, ', ') as warehouse_sizes,
        avg(auto_stop_mins) as avg_auto_stop_mins,
        min(creation_time) as first_warehouse_created,
        max(creation_time) as last_warehouse_created
    from warehouses 
    where deleted_time is null
    group by account_id, workspace_id
),

-- Aggregate job information per workspace  
job_summary as (
    select 
        account_id,
        workspace_id,
        count(*) as total_jobs,
        count(case when schedule is not null then 1 end) as scheduled_jobs,
        count(distinct creator_user_name) as unique_job_creators,
        avg(max_concurrent_runs) as avg_max_concurrent_runs,
        min(created_time) as first_job_created,
        max(created_time) as last_job_created
    from jobs
    group by account_id, workspace_id
),

-- Enhanced workspace metadata
workspace_enhanced as (
    select 
        w.account_id,
        w.workspace_id,
        w.workspace_name,
        w.deployment_name,
        w.workspace_status,
        w.creation_time as workspace_creation_time,
        w.workspace_type,
        w.cloud,
        w.region,
        w.pricing_tier,
        w.is_no_public_ip_enabled,
        
        -- Workspace age calculations
        current_date() - date(w.creation_time) as workspace_age_days,
        case 
            when current_date() - date(w.creation_time) < 30 then 'New (< 30 days)'
            when current_date() - date(w.creation_time) < 180 then 'Recent (< 6 months)'
            when current_date() - date(w.creation_time) < 365 then 'Mature (< 1 year)'
            else 'Established (> 1 year)'
        end as workspace_age_category,
        
        -- Security and compliance flags
        case 
            when w.is_no_public_ip_enabled then 'Private'
            else 'Public'
        end as network_security_type,
        
        case 
            when w.storage_customer_managed_key_id is not null then 'Customer Managed'
            else 'Databricks Managed'
        end as storage_encryption_type,
        
        -- Warehouse metrics
        coalesce(whs.total_warehouses, 0) as total_warehouses,
        coalesce(whs.running_warehouses, 0) as running_warehouses,
        coalesce(whs.serverless_warehouses, 0) as serverless_warehouses,
        coalesce(whs.photon_enabled_warehouses, 0) as photon_enabled_warehouses,
        whs.warehouse_sizes,
        whs.avg_auto_stop_mins,
        
        -- Job metrics
        coalesce(js.total_jobs, 0) as total_jobs,
        coalesce(js.scheduled_jobs, 0) as scheduled_jobs,
        coalesce(js.unique_job_creators, 0) as unique_job_creators,
        js.avg_max_concurrent_runs,
        
        -- Activity indicators
        case 
            when coalesce(whs.total_warehouses, 0) > 0 
                 or coalesce(js.total_jobs, 0) > 0 then 'Active'
            else 'Inactive'
        end as workspace_activity_status,
        
        -- Custom tags parsing (if needed for cost allocation)
        w.custom_tags,
        
        current_timestamp() as silver_processed_at
        
    from workspaces w
    left join warehouse_summary whs 
        on w.workspace_id = whs.workspace_id 
        and w.account_id = whs.account_id
    left join job_summary js 
        on w.workspace_id = js.workspace_id 
        and w.account_id = js.account_id
)

select * from workspace_enhanced
