model (
    name sqlmesh__gas_prices_spa.stg__regions,
    kind view,
);

with

source as (
    select
        cast(id as integer) as id,
        trim(name) as name,

    from `gas_prices_spa.raw_regions`
)

select * from source
