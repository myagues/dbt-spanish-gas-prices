with

source as (
    select
        cast(id as integer) as id,
        cast(region_id as integer) as region_id,
        trim(name) as name,

    from {{ source('raw_data', 'raw_provinces') }}
)

select * from source
