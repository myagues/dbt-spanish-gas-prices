with

source as (
    select
        cast(id as integer) as id,
        trim(name) as name,

    from {{ source('raw_data', 'raw_regions') }}
)

select * from source
