with

source as (

    select * from {{ source('raw_data', 'raw_provinces') }}

),

final as (

    select
        cast(id as integer) as id,
        cast(region_id as integer) as region_id,
        trim(name) as name

    from source

)

select * from final
