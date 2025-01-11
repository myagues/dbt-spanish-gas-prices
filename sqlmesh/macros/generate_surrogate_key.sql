{% macro generate_surrogate_key(field_list) %}
    {%- set default_null_value = '_dbt_utils_surrogate_key_null_' -%}

    {%- set fields = [] -%}

    {%- for field in field_list -%}

        {%- do fields.append(
            "coalesce(cast(" ~ field ~ " as " ~ "string), '" ~ default_null_value  ~"')"
        ) -%}

        {%- if not loop.last %}
            {%- do fields.append("'-'") -%}
        {%- endif -%}

    {%- endfor -%}

    to_hex(md5(concat({{ fields|join(', ')}} )))

{% endmacro %}
