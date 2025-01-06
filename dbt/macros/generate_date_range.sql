{% macro generate_date_range(start_date_iso, end_date_iso, granularity='day') %}
    {% set date_range_daily = dates_in_range(
        start_date_iso,
        end_date_iso,
        in_fmt="%Y-%m-%d",
        out_fmt="'%Y-%m-%d'",
    ) %}

    {% if granularity == 'day' %}

        {{ return(date_range_daily) }}

    {% elif granularity == 'month' %}

        {% set date_range_monthly = [] %}
        {% for day in date_range_daily %}
            {% do date_range_monthly.append(day) if modules.re.search("-01'$", day) %}
        {% endfor %}
        {{ return(date_range_monthly) }}

    {% else %}

        {{ exceptions.raise_compiler_error(
            "Invalid 'granularity' value. Available options are: 'day', 'month'."
        ) }}

    {% endif %}
{% endmacro %}
