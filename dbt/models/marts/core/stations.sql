{{ config(materialized="incremental", unique_key="station_id") }}

with

station_info as (
    select
        date,
        station_id,
        name,
        address,
        town,
        zip_code,
        longitude,
        latitude,
        road_side,
        restriction,
        sender,
        schedule,
        region_id,
        province_id,
        municipality_id

    from {{ ref("stg__gas_prices") }}

    {% if is_incremental() %}

        where date_trunc(date, month)
            between '{{ var("start_date") }}'
            and '{{ var("end_date") }}'

    {% endif %}
),

unique_station_info as (
    {{
        dbt_utils.deduplicate(
            relation="station_info",
            partition_by="station_id",
            order_by="date",
        )
    }}
),

regions as (
    select
        id as region_id,
        name as region_name

    from {{ ref("stg__regions") }}
),

provinces as (
    select
        id as province_id,
        name as province_name

    from {{ ref("stg__provinces") }}
),

municipalities as (
    select
        id as municipality_id,
        name as municipality_name

    from {{ ref("stg__municipalities") }}
),

final as (
    select
        station_info.address,
        station_info.latitude,
        station_info.longitude,
        station_info.municipality_id,
        station_info.name,
        station_info.province_id,
        station_info.region_id,
        station_info.restriction,
        station_info.road_side,
        station_info.schedule,
        station_info.sender,
        station_info.station_id,
        station_info.town,
        station_info.zip_code,

        municipalities.municipality_name,
        provinces.province_name,
        regions.region_name,

    from unique_station_info as station_info
    left join regions using (region_id)
    left join provinces using (province_id)
    left join municipalities using (municipality_id)
)

select * from final
