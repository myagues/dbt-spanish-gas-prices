-- https://docs.getdbt.com/reference/resource-configs/bigquery-configs#merge-behavior-incremental-models
{% set partitions_to_replace = ["date_trunc(current_date, month)"] %}
{{
    config(
        materialized="incremental",
        incremental_strategy="insert_overwrite",
        partition_by={"field": "date", "data_type": "date", "granularity": "month"},
        partitions=partitions_to_replace,
    )
}}

with

month_partition as (

    select
        station_id,
        date,
        -- price_id,
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

    from {{ ref("stg_gas_prices") }}

    {% if is_incremental() %}

        where date_trunc(date, month) in ({{ partitions_to_replace | join(",") }})

    {% endif %}

)

{{
    dbt_utils.deduplicate(
        relation="month_partition",
        partition_by="station_id, date",
        order_by="date desc",
    )
}}
