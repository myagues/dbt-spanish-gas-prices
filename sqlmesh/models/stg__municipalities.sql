model (
    name sqlmesh__gas_prices_spa.stg__municipalities,
    kind view,
);

with

source as (
    select
        cast(id as integer) as id,
        cast(province_id as integer) as province_id,
        trim(name) as name,

    from `gas_prices_spa.raw_municipalities`
)

select * from source
