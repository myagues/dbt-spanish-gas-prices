with

stations as (

    select * from {{ ref('stations') }}

),

station_dates as (

    select
        station_id,
        min(date) as min_date,
        max(date) as max_date

    from {{ ref('prices__deduped') }}
    group by 1

),

final as (

    select
        {{ dbt_utils.star(from=ref('stations')) }},
        station_dates.min_date as min_date,
        station_dates.max_date as max_date

    from stations
    left join station_dates using (station_id)

)

select * from final
