model (
    name sqlmesh__gas_prices_spa.fct__gas_prices,
    kind incremental_by_time_range (
        time_column date
    ),
    partitioned_by timestamp_trunc(date, month),
    physical_properties (
        require_partition_filter = true,
    ),
);

with

final as (
    select
        date,
        station_id,
        price_id,

        biodiesel,
        bioetanol,
        cng,
        diesel_A,
        diesel_B,
        diesel_premium,
        gasoline_95E10,
        gasoline_95E5_premium,
        gasoline_95E5,
        gasoline_98E10,
        gasoline_98E5,
        hydrogen,
        lng,
        lpg,
        perc_bioetanol,
        perc_methyl_ester,

    from sqlmesh__gas_prices_spa.stg__gas_prices

    where date_trunc(date, month) between @start_ds and @end_ts
),

deduplicated as (
    select unique.* except (price_id)
    from (
        select
            array_agg(final order by date limit 1)[offset(0)] as unique
        from final
        group by price_id
    )
)

select * from deduplicated
