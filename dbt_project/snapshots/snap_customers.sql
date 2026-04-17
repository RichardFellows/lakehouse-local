{#
    SCD Type 2 snapshot of the customer dimension.

    Reads the current-day snapshot dumped by the `daily_snapshot_ingest` DAG.
    Uses the `timestamp` strategy on `updated_at` — the source system bumps
    this column whenever a row changes, so we can detect updates without
    having to diff every column.

    After a run, this table contains one row per (customer_id, version):
      * Rows with `dbt_valid_to IS NULL` are the current version.
      * Rows with `dbt_valid_to IS NOT NULL` are closed historical versions.
#}
{% snapshot snap_customers %}

    {{
        config(
          target_schema='snapshots',
          unique_key='customer_id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True,
        )
    }}

    select
        cast(customer_id as int) as customer_id,
        first_name,
        last_name,
        email,
        cast(created_at as date) as created_at,
        cast(updated_at as timestamp) as updated_at
    from
    {%- if target.name == 'spark' %}
        {{ source('raw', 'customers_latest') }}
    {%- else %}
        {{ ref('customers_latest') }}
    {%- endif %}

{% endsnapshot %}
