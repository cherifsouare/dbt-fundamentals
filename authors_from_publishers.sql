{% macro authors_from_publishers(column_name) -%}
    case
       when {{ column_name }} > 0 
       then 'yes'
       else 'no'
    end
{%- endmacro %}


