{#
    SCD Type 2 snapshot of the orders fact.

    Orders are semi-mutable: `status` transitions (pending → completed, etc.)
    bump `updated_at`. The SCD2 history lets you reconstruct "what was the
    status of this order on any given day?".
#}
{% snapshot snap_orders %}

    {{
        config(
          target_schema='snapshots',
          unique_key='order_id',
          strategy='timestamp',
          updated_at='updated_at',
          invalidate_hard_deletes=True,
        )
    }}

    select
        cast(order_id as int) as order_id,
        cast(customer_id as int) as customer_id,
        cast(order_date as date) as order_date,
        cast(amount as decimal(10, 2)) as amount,
        status,
        cast(updated_at as timestamp) as updated_at
    from
    {%- if target.name == 'spark' %}
        {{ source('raw', 'orders_latest') }}
    {%- else %}
        {{ ref('orders_latest') }}
    {%- endif %}

{% endsnapshot %}
