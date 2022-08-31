{{
    config(
        materialized='incremental',
        unique_key='station_id'
    )
}}


with

station_info as (

    select
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

    from {{ ref('stg_gas_prices') }}

    {% if is_incremental() %}

        where date in (current_date)

    {% endif %}

),

regions as (

    select
        id as region_id,
        name as region_name

    from {{ ref('stg_regions') }}

),

provinces as (

    select
        id as province_id,
        name as province_name

    from {{ ref('stg_provinces') }}

),

municipalities as (

    select
        id as municipality_id,
        name as municipality_name

    from {{ ref('stg_municipalities') }}

),

final as (

    select
        station_info.station_id as station_id,
        station_info.name as name,
        station_info.address as address,
        station_info.town as town,
        station_info.zip_code as zip_code,
        station_info.longitude as longitude,
        station_info.latitude as latitude,
        station_info.road_side as road_side,
        station_info.restriction as restriction,
        station_info.sender as sender,
        station_info.schedule as schedule,
        station_info.region_id as region_id,
        regions.region_name as region_name,
        station_info.province_id as province_id,
        provinces.province_name as province_name,
        station_info.municipality_id as municipality_id,
        municipalities.municipality_name as municipality_name

    from station_info
    left join regions using (region_id)
    left join provinces using (province_id)
    left join municipalities using (municipality_id)

)

select * from final
