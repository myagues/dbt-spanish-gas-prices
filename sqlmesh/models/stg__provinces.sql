model (
    name sqlmesh__gas_prices_spa.stg__provinces,
    kind view,
);

with

source as (
    select
        cast(id as integer) as id,
        cast(region_id as integer) as region_id,
        trim(name) as name,

    from `gas_prices_spa.raw_provinces`
)

select * from source
