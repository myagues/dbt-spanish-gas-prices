with

source as (

    select * from {{ source('raw_data', 'raw_gas_prices') }}

),

daily_station_prices as (

    select
        cast(station_id as integer) as station_id,
        date,
        {{ dbt_utils.surrogate_key(['station_id', 'date']) }} as price_id,

        -- station info
        trim(name) as name,
        trim(address) as address,
        trim(town) as town,
        trim(zip_code) as zip_code,
        safe_cast(replace(longitude, ',', '.') as float64) as longitude,
        safe_cast(replace(latitude, ',', '.') as float64) as latitude,
        trim(road_side) as road_side,
        trim(restriction) as restriction,
        trim(sender) as sender,
        trim(schedule) as schedule,
        cast(region_id as integer) as region_id,
        cast(province_id as integer) as province_id,
        cast(municipality_id as integer) as municipality_id,

        -- station prices
        {{ safe_cast_numeric('gasoline_95E5') }} as gasoline_95E5,
        {{ safe_cast_numeric('gasoline_95E5_premium') }} as gasoline_95E5_premium,
        {{ safe_cast_numeric('gasoline_95E10') }} as gasoline_95E10,
        {{ safe_cast_numeric('gasoline_98E5') }} as gasoline_98E5,
        {{ safe_cast_numeric('gasoline_98E10') }} as gasoline_98E10,
        {{ safe_cast_numeric('diesel_A') }} as diesel_A,
        {{ safe_cast_numeric('diesel_B') }} as diesel_B,
        {{ safe_cast_numeric('diesel_premium') }} as diesel_premium,
        {{ safe_cast_numeric('bioetanol') }} as bioetanol,
        {{ safe_cast_numeric('biodiesel') }} as biodiesel,
        {{ safe_cast_numeric('perc_bioetanol') }} as perc_bioetanol,
        {{ safe_cast_numeric('perc_methyl_ester') }} as perc_methyl_ester,
        {{ safe_cast_numeric('lpg') }} as lpg,
        {{ safe_cast_numeric('cng') }} as cng,
        {{ safe_cast_numeric('lng') }} as lng,
        {{ safe_cast_numeric('hydrogen') }} as hydrogen

    from source

)

select * from daily_station_prices
