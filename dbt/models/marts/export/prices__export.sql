with

final as (
    select
        {{ dbt_utils.star(ref('prices'), except=["perc_bioetanol", "perc_methyl_ester"]) }},
        {{ dbt_utils.star(ref('stations'), except=["station_id"]) }}

    from {{ ref('prices') }}
    left join {{ ref('stations') }} using (station_id)
)

select * from final
