{% snapshot snap_saas_accounts %}

{{
    config(
      target_schema='SNAPSHOTS',
      unique_key='account_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

select
    account_id,
    company_name,
    plan_tier,
    mrr_usd,
    contract_renewal_date,
    updated_at
from {{ ref('stg_saas_accounts') }}

{% endsnapshot %}