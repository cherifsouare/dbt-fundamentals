{% macro scienceradar_bins(column_name) -%}
    case
       when {{ column_name }} is null
       then 'not available'
       when {{ column_name }} <= 0.0001
       then 'untrusted data'
       when {{ column_name }} < 0.1
       then '0.0001 - 0.09'
       when {{ column_name }} < 0.2
       then '0.1 - 0.19'
       when {{ column_name }} < 0.3
       then '0.2 - 0.29'
       when {{ column_name }} < 0.4
       then '0.3 - 0.39'
       when {{ column_name }} < 0.5
       then '0.4 - 0.49'
       when {{ column_name }} < 0.6
       then '0.5 - 0.59'
       when {{ column_name }} < 0.7
       then '0.6 - 0.69'
       when {{ column_name }} < 0.8
       then '0.7 - 0.79'
       when {{ column_name }} < 0.9
       then '0.8 - 0.89'
       else '0.9 - 1'
    end
{%- endmacro %}


