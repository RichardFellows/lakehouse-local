{#
    Current-version order view — open row per order_id from the SCD2 snapshot.
    If an order's status has flipped (e.g. pending → completed), this view shows
    the latest status; the historical status transitions are preserved in
    `snap_orders` / `dim_customer_history` / `fact_orders`.
#}
{{ config(materialized='view') }}

select
    order_id,
    customer_id,
    order_date,
    amount,
    status,
    updated_at
from {{ ref('snap_orders') }}
where dbt_valid_to is null
