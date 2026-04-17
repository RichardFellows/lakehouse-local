{#
    Current-version customer view — pulls the open row (dbt_valid_to IS NULL)
    from the SCD2 snapshot. Always reflects the latest-ingested snapshot.
#}
{{ config(materialized='view') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at
from {{ ref('snap_customers') }}
where dbt_valid_to is null
