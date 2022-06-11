{{
    config(
        materialized='view',
        sort='date'
    )
}}

with stations as (
    select * from {{ ref('dim_stations') }}
),

region as (
    select
        cast(id as integer) as region_id,
        name as region_name
    from {{ source('raw_data', 'region') }}
),

province as (
    select
        cast(id as integer) as province_id,
        name as province_name
    from {{ source('raw_data', 'province') }}
),

municipality as (
    select
        cast(id as integer) as municipality_id,
        name as municipality_name
    from {{ source('raw_data', 'municipality') }}
),

prices as (
    select * from {{ ref('prices__deduped') }}
),

final as (
    select
        {{ dbt_utils.star(ref('dim_stations')) }},
        region_name,
        province_name,
        municipality_name,
        {{ dbt_utils.star(from=ref('prices__deduped'), except=["station_id"]) }}
    from prices
    left join stations using (station_id)
    left join region using (region_id)
    left join province using (province_id)
    left join municipality using (municipality_id)
)

select * from final
