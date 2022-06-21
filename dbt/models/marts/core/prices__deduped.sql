{{
    config(materialized='view')
}}

with

final as (

    select
        {{ dbt_utils.star(from=ref('prices'), except=["price_id", "date"]) }},
        min(date) as date

    from (
        select
            {{ dbt_utils.star(ref('prices')) }},
            row_number() over (order by date, station_id, {{ dbt_utils.star(from=ref('prices'), except=["latitude", "longitude", "price_id", "date", "station_id"]) }}) -
            row_number() over (partition by station_id, {{ dbt_utils.star(from=ref('prices'), except=["latitude", "longitude", "price_id", "date", "station_id"]) }} order by date)
            as grp
        from {{ ref('prices') }}
    )

    group by
        station_id,
        {{ dbt_utils.star(from=ref('prices'), except=["price_id", "date", "station_id"]) }}

)

select * from final
