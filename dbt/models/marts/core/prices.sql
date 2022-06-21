with

final as (

    select
        station_id,
        date,
        price_id,
        gasoline_95E5,
        gasoline_95E5_premium,
        gasoline_95E10,
        gasoline_98E5,
        gasoline_98E10,
        diesel_A,
        diesel_B,
        diesel_premium,
        bioetanol,
        biodiesel,
        perc_bioetanol,
        perc_methyl_ester,
        lpg,
        cng,
        lng,
        hydrogen

    from {{ ref('stg_gas_prices') }}

)


{{ dbt_utils.deduplicate(
    relation='final',
    partition_by="price_id",
    order_by="date asc"
   )
}}
