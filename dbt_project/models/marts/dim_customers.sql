with accounts as (
    select
        account_id,
        company_name,
        plan_tier,
        mrr_usd,
        contract_renewal_date,
        updated_at as account_updated_at
    from {{ ref('stg_saas_accounts') }}
),

account_snapshot_stats as (
    select
        account_id,
        count(*) as account_snapshot_versions,
        count(distinct plan_tier) as distinct_plan_tiers_seen,
        min(dbt_valid_from) as first_account_snapshotted_at,
        max(dbt_valid_from) as latest_account_snapshot_at
    from {{ ref('snap_saas_accounts') }}
    group by account_id
),

ticket_rollups as (
    select
        account_id,
        count(*) as total_tickets,
        count_if(lower(ticket_status) <> 'closed') as open_tickets,
        count_if(escalation_risk_tier = 'High Escalation Risk') as high_escalation_risk_tickets,
        coalesce(sum(comments_count), 0) as total_ticket_comments,
        max(created_at_ts) as latest_ticket_created_at
    from {{ ref('mart_ticket_escalation_risk') }}
    where account_id is not null
    group by account_id
),

final as (
    select
        a.account_id,
        a.company_name,
        a.plan_tier,
        a.mrr_usd,
        a.contract_renewal_date,
        a.account_updated_at,
        coalesce(s.account_snapshot_versions, 0) as account_snapshot_versions,
        coalesce(s.distinct_plan_tiers_seen, 1) as distinct_plan_tiers_seen,
        s.first_account_snapshotted_at,
        s.latest_account_snapshot_at,
        coalesce(tr.total_tickets, 0) as total_tickets,
        coalesce(tr.open_tickets, 0) as open_tickets,
        coalesce(tr.high_escalation_risk_tickets, 0) as high_escalation_risk_tickets,
        coalesce(tr.total_ticket_comments, 0) as total_ticket_comments,
        tr.latest_ticket_created_at,
        case
            when coalesce(tr.high_escalation_risk_tickets, 0) > 0
                 and upper(a.plan_tier) = 'ENTERPRISE' then 'Critical Revenue Risk'
            when coalesce(tr.high_escalation_risk_tickets, 0) > 0 then 'Elevated Churn Risk'
            when coalesce(tr.open_tickets, 0) > 0 then 'Monitor'
            else 'Healthy'
        end as customer_health_status,
        current_timestamp() as dbt_updated_at
    from accounts a
    left join account_snapshot_stats s
        on a.account_id = s.account_id
    left join ticket_rollups tr
        on a.account_id = tr.account_id
)

select *
from final