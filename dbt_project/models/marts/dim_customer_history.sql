{#
    Full SCD2 customer history. One row per (customer_id, version).

    `version` is a 1-based ordinal per customer so you can join back to facts
    by "version 1" / "version 2" if you're explicit about point-in-time. Use
    `valid_from` / `valid_to` for range queries:

        select ...
        from {{ ref('dim_customer_history') }}
        where valid_from <= <target_date>
          and (valid_to is null or valid_to > <target_date>)
#}
{{ config(materialized='table') }}

select
    customer_id,
    first_name,
    last_name,
    email,
    created_at,
    updated_at as source_updated_at,
    dbt_valid_from as valid_from,
    dbt_valid_to   as valid_to,
    case when dbt_valid_to is null then true else false end as is_current,
    row_number() over (
        partition by customer_id
        order by dbt_valid_from
    ) as version
from {{ ref('snap_customers') }}
