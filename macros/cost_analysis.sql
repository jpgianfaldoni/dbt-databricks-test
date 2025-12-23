-- Cost analysis macros for business calculations

{% macro calculate_efficiency_score(cpu_percent, memory_percent) %}
    round(({{ cpu_percent }} + {{ memory_percent }}) / 2.0, 2)
{% endmacro %}

{% macro get_optimization_recommendation(efficiency_score, low_util_hours, high_util_hours, max_cpu, max_memory) %}
    case 
        when {{ efficiency_score }} < 30 and {{ low_util_hours }} > 12 then
            'Consider downsizing cluster or implementing auto-scaling'
        when {{ efficiency_score }} > 70 and {{ low_util_hours }} > 8 then
            'Implement aggressive auto-stop policies for off-hours'
        when {{ max_cpu }} < 50 and {{ max_memory }} < 50 then
            'Cluster appears oversized - consider smaller node types'
        when {{ efficiency_score }} between 30 and 50 then
            'Monitor usage patterns and consider right-sizing'
        else 'Cluster utilization appears optimal'
    end
{% endmacro %}

{% macro calculate_waste_score(efficiency_score) %}
    round(
        case 
            when {{ efficiency_score }} < 20 then 80 + (20 - {{ efficiency_score }})
            when {{ efficiency_score }} < 50 then 50 + (50 - {{ efficiency_score }}) * 0.6
            else greatest(0, 30 - {{ efficiency_score }} * 0.6)
        end,
        2
    )
{% endmacro %}

{% macro get_usage_pattern_classification(weekend_usage_pct) %}
    case 
        when {{ weekend_usage_pct }} > 30 then '24/7 Operations'
        when {{ weekend_usage_pct }} > 10 then 'Some Weekend Activity'
        else 'Business Hours Only'
    end
{% endmacro %}

{% macro get_volatility_assessment(volatility, avg_usage) %}
    case 
        when {{ volatility }} / nullif({{ avg_usage }}, 0) > 1.5 then 'Highly Variable'
        when {{ volatility }} / nullif({{ avg_usage }}, 0) > 0.8 then 'Moderately Variable'
        else 'Stable'
    end
{% endmacro %}
