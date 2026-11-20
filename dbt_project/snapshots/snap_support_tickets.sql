{% snapshot snap_support_tickets %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='ticket_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select
    ticket_id,
    ticket_number,
    ticket_title,
    ticket_status,
    comments_count,
    customer_handle,
    created_at,
    updated_at,
    closed_at
from {{ ref('stg_support_tickets') }}

{% endsnapshot %}