{#
    Every order in fact_orders must have resolved to exactly one customer
    version (the as-of join). If the join matched zero or >1 versions we
    have a gap or overlap in the SCD2 history.

    This assertion is scoped to orders whose customer_id exists in the
    current customer dimension (`dim_customer_current`) — i.e. we don't
    fail if the source drops a customer mid-history, which is a hard-delete
    case that `invalidate_hard_deletes=True` on the snapshot already handles.
#}
with customer_history as (
    select
        customer_id,
        case
            when row_number() over (partition by customer_id order by dbt_valid_from) = 1
                then cast('1900-01-01' as timestamp)
            else dbt_valid_from
        end as effective_from,
        dbt_valid_to as effective_to
    from {{ ref('snap_customers') }}
),

order_counts as (
    select
        o.order_id,
        count(c.customer_id) as matched_versions
    from {{ ref('stg_orders') }} o
    join {{ ref('dim_customer_current') }} dc
        on dc.customer_id = o.customer_id
    left join customer_history c
        on c.customer_id = o.customer_id
       and cast(o.order_date as timestamp) >= c.effective_from
       and (c.effective_to is null
            or cast(o.order_date as timestamp) < c.effective_to)
    group by o.order_id
)
select *
from order_counts
where matched_versions <> 1
