with

prices as (

    select * from {{ ref('prices') }}

),

stations as (

    select * from {{ ref('stations') }}

),

final as (

    select
        {{ dbt_utils.star(ref('prices'), except=["price_id", "perc_bioetanol", "perc_methyl_ester"]) }},
        {{ dbt_utils.star(ref('stations'), except=["station_id"]) }}

    from prices
    left join stations using (station_id)

)

select * from final
