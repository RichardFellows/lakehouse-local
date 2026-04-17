{#
    SCD2 integrity — every customer_id must have exactly one open row
    (dbt_valid_to IS NULL) in the snapshot.
#}
select
    customer_id,
    count(*) as open_row_count
from {{ ref('snap_customers') }}
where dbt_valid_to is null
group by customer_id
having count(*) <> 1
