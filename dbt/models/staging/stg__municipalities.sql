with

source as (
    select
        cast(id as integer) as id,
        cast(province_id as integer) as province_id,
        trim(name) as name,

    from {{ source('raw_data', 'raw_municipalities') }}
)

select * from source
