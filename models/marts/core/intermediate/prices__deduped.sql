select
    {{ dbt_utils.star(from=ref('fct_prices'), except=["price_id", "date"]) }},
    min(date) as date
from (
    select
        {{ dbt_utils.star(ref('fct_prices')) }},
        row_number() over (order by date, station_id, {{ dbt_utils.star(from=ref('fct_prices'), except=["price_id", "date", "station_id"]) }}) -
        row_number() over (partition by station_id, {{ dbt_utils.star(from=ref('fct_prices'), except=["price_id", "date", "station_id"]) }} order by date)
        as grp
    from {{ ref('fct_prices') }}
)
group by
    station_id,
    {{ dbt_utils.star(from=ref('fct_prices'), except=["price_id", "date", "station_id"]) }}
