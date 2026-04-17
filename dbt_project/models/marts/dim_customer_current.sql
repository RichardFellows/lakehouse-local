{#
    Current-version customer dimension. One row per customer_id. Alias of
    stg_customers materialized as a table for BI/notebook consumption.
#}
{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at as source_updated_at
from {{ ref('stg_customers') }}
