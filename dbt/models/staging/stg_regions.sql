with

source as (

    select * from {{ source('raw_data', 'raw_regions') }}

),

final as (

    select
        cast(id as integer) as id,
        trim(name) as name

    from source

)

select * from final
