{{
    config(
        materialized='incremental',
        unique_key='price_id',
    )
}}

{{ dbt_utils.deduplicate(
    relation=ref('stg_prices'),
    group_by="price_id",
    order_by="date asc"
   )
}}
