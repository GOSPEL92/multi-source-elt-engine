with source as (
    select *
    from {{ source('raw_saas_data', 'raw_saas_accounts') }}
),

deduped as (
    select
        cast(account_id as varchar) as account_id,
        trim(cast(company_name as varchar)) as company_name,
        initcap(trim(cast(plan_tier as varchar))) as plan_tier,
        cast(mrr_usd as number(12, 2)) as mrr_usd,
        cast(contract_renewal_date as date) as contract_renewal_date,
        cast(updated_at as timestamp_tz) as updated_at,
        row_number() over (
            partition by cast(account_id as varchar)
            order by cast(updated_at as timestamp_tz) desc
        ) as row_num
    from source
    where account_id is not null
)

select
    account_id,
    company_name,
    plan_tier,
    mrr_usd,
    contract_renewal_date,
    updated_at
from deduped
where row_num = 1