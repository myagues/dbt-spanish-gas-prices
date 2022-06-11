select
    cast(station_id as integer) station_id,
    name,
    address,
    town,
    zip_code,
    safe_cast(replace(longitude, ',', '.') as float64) as longitude,
    safe_cast(replace(latitude, ',', '.') as float64) as latitude,
    road_side,
    restriction,
    sender,
    schedule,
    cast(region_id as INTEGER) as region_id,
    cast(province_id as INTEGER) as province_id,
    cast(municipality_id as INTEGER) as municipality_id

from {{ source('raw_data', 'prices') }}
