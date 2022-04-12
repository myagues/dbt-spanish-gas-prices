{{
    config(
        materialized='incremental',
        unique_key='station_id',
    )
}}

{{ dbt_utils.deduplicate(
    relation=ref('stg_stations'),
    group_by="station_id",
    order_by="station_id asc"
   )
}}
