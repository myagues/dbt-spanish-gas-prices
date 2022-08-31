
{% macro safe_cast_numeric(column_name) %}
    safe_cast(replace({{ column_name }}, ',', '.') as numeric)
{% endmacro %}
