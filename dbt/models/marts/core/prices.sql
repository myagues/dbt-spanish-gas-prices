{%
    set partitions_to_replace = generate_date_range(
        var("start_date"),
        var("end_date"),
        granularity='month'
    )
%}
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "date", "data_type": "date", "granularity": "month"},
        partitions=partitions_to_replace,
    )
}}

with

final as (
    select
        date,
        station_id,

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

    from {{ ref("stg__gas_prices") }}

    {% if is_incremental() %}

        where date_trunc(date, month)
            between '{{ var("start_date") }}'
            and '{{ var("end_date") }}'

    {% endif %}
)

select * from final
