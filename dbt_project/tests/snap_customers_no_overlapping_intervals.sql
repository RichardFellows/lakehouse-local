{#
    SCD2 integrity — validity intervals for a given customer_id must not
    overlap. For every pair of versions (a, b) on the same key, one must
    close before the other opens.
#}
with versions as (
    select
        customer_id,
        dbt_valid_from as valid_from,
        coalesce(dbt_valid_to, cast('9999-12-31' as timestamp)) as valid_to
    from {{ ref('snap_customers') }}
)

select
    a.customer_id,
    a.valid_from as a_from, a.valid_to as a_to,
    b.valid_from as b_from, b.valid_to as b_to
from versions a
join versions b
  on a.customer_id = b.customer_id
 and a.valid_from < b.valid_from
where a.valid_to > b.valid_from
