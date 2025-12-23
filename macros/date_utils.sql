-- Date utility macros for the dbt project

{% macro get_business_hours_filter() %}
    extract(hour from timestamp) between 8 and 18
    and extract(dow from timestamp) not in (1, 7)
{% endmacro %}

{% macro get_date_spine(start_date, end_date) %}
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="'" + start_date + "'",
        end_date="'" + end_date + "'"
    ) }}
{% endmacro %}

{% macro calculate_usage_category(usage_quantity, thresholds) %}
    case 
        when {{ usage_quantity }} >= {{ thresholds.high }} then 'High'
        when {{ usage_quantity }} >= {{ thresholds.medium }} then 'Medium'
        else 'Low'
    end
{% endmacro %}

{% macro get_service_category_from_sku(sku_column) %}
    case 
        when {{ sku_column }} like '%JOBS%' then 'Jobs Compute'
        when {{ sku_column }} like '%ALL_PURPOSE%' then 'Interactive Compute'
        when {{ sku_column }} like '%SQL%' then 'SQL Warehouse'
        when {{ sku_column }} like '%DLT%' then 'Delta Live Tables'
        when {{ sku_column }} like '%SERVERLESS%' then 'Serverless'
        when {{ sku_column }} like '%STORAGE%' then 'Storage'
        else 'Other'
    end
{% endmacro %}

{% macro get_compute_type_from_sku(sku_column) %}
    case 
        when {{ sku_column }} like '%PHOTON%' then 'Photon'
        when {{ sku_column }} like '%STANDARD%' then 'Standard'
        when {{ sku_column }} like '%PREMIUM%' then 'Premium'
        else 'Unknown'
    end
{% endmacro %}
