{{ config(materialized='table') }}

with customers as (
    select * from {{ ref('stg_customers') }}
),

orders as (
    select * from {{ ref('stg_orders') }}
),

customer_orders as (
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
from customer_orders
