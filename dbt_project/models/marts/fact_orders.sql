{#
    Fact table joining each order to the **point-in-time** customer attributes
    that were valid on that order's `order_date`. This is the canonical SCD2
    use case: reporting against attributes as they were when the event happened,
    not as they are today.

    Uses current order status from `stg_orders` (one row per order). If you
    need the historical status transitions, query `snap_orders` directly.
#}
{{ config(materialized='table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

customer_history as (
    -- Back-date the earliest observed version of each customer to the
    -- beginning of time. Without this, orders placed before the first
    -- snapshot-run date would fail to match any version (the initial
    -- `dbt_valid_from` is set to the source `updated_at`, which is only
    -- as old as the first feed). With the back-date, every order falls
    -- inside exactly one version's effective interval.
    select
        customer_id,
        first_name,
        last_name,
        email,
        case
            when row_number() over (partition by customer_id order by dbt_valid_from) = 1
                then cast('1900-01-01' as timestamp)
            else dbt_valid_from
        end as effective_from,
        dbt_valid_to as effective_to,
        dbt_valid_from as original_valid_from,
        dbt_valid_to   as original_valid_to
    from {{ ref('snap_customers') }}
)

select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.amount,
    o.status,
    o.updated_at as order_updated_at,
    c.first_name              as customer_first_name_asof,
    c.last_name               as customer_last_name_asof,
    c.email                   as customer_email_asof,
    c.original_valid_from     as customer_version_valid_from,
    c.original_valid_to       as customer_version_valid_to
from orders o
left join customer_history c
    on c.customer_id = o.customer_id
   and cast(o.order_date as timestamp) >= c.effective_from
   and (c.effective_to is null or cast(o.order_date as timestamp) < c.effective_to)
