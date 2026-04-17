{#
    Customer-level rollup with tiering. Kept backwards-compatible with the
    pre-SCD2 version of this mart (same column contract) so the 4-engine
    parity notebook (explore.py) continues to work. Under the hood it now
    sources from fact_orders + dim_customer_current so every number lines
    up with the SCD2 history.
#}
{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('dim_customer_current') }}
),

orders as (
    select * from {{ ref('fact_orders') }}
),

rollup as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        count(o.order_id) as total_orders,
        sum(case when o.status = 'completed' then o.amount else 0 end) as total_revenue,
        min(o.order_date) as first_order_date,
        max(o.order_date) as last_order_date
    from customers c
    left join orders o on c.customer_id = o.customer_id
    group by c.customer_id, c.first_name, c.last_name, c.email
)

select
    *,
    case
        when total_orders >= 3 then 'high'
        when total_orders >= 2 then 'medium'
        else 'low'
    end as customer_tier
from rollup
