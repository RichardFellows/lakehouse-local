{#
    Customer Lifetime Value — RFM (Recency / Frequency / Monetary) rollup per
    current customer. Built on fact_orders so "customer_email_asof" etc. reflect
    the attributes at each order's date. The top-line aggregates still group
    by customer_id, which is stable across SCD2 versions.
#}
{{ config(materialized='table') }}

with fact as (
    select * from {{ ref('fact_orders') }}
    where status = 'completed'
),

agg as (
    select
        customer_id,
        count(order_id) as frequency,
        sum(amount) as monetary,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date
    from fact
    group by customer_id
),

customers as (
    select * from {{ ref('dim_customer_current') }}
)

select
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    coalesce(a.frequency, 0) as frequency,
    coalesce(a.monetary, cast(0 as decimal(10, 2))) as monetary,
    a.first_order_date,
    a.last_order_date,
    case
        when coalesce(a.monetary, 0) >= 500 then 'platinum'
        when coalesce(a.monetary, 0) >= 200 then 'gold'
        when coalesce(a.monetary, 0) >  0  then 'silver'
        else 'dormant'
    end as ltv_bucket
from customers c
left join agg a using (customer_id)
