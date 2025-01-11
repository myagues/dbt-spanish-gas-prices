model (
    name sqlmesh__gas_prices_spa.fct__stations,
    kind incremental_by_unique_key (
        unique_key station_id
    ),
);

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
        municipality_id,

    from sqlmesh__gas_prices_spa.stg__gas_prices

    where date_trunc(date, month) between @start_ds and @end_ts
),

unique_station_info as (
    select unique.*
    from (
        select
            array_agg(station_info order by date limit 1)[offset(0)] as unique
        from station_info
        group by station_id
    )
),

regions as (
    select
        id as region_id,
        name as region_name,

    from sqlmesh__gas_prices_spa.stg__regions
),

provinces as (
    select
        id as province_id,
        name as province_name,

    from sqlmesh__gas_prices_spa.stg__provinces
),

municipalities as (
    select
        id as municipality_id,
        name as municipality_name,

    from sqlmesh__gas_prices_spa.stg__municipalities
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
