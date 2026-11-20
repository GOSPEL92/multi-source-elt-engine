with daily_ops as (
    select
        activity_date,
        total_tickets_updated,
        open_tickets_count,
        closed_tickets_count,
        total_customer_interactions,
        avg_billing_index_usd,
        total_market_liquidity_usd,
        dbt_updated_at
    from {{ ref('fct_daily_saas_operations') }}
    where activity_date is not null
),

with_trends as (
    select
        activity_date,
        total_tickets_updated,
        open_tickets_count,
        closed_tickets_count,
        total_customer_interactions,
        avg_billing_index_usd,
        total_market_liquidity_usd,

        -- Backlog & customer effort metrics
        round(
            open_tickets_count::float / nullif(total_tickets_updated, 0),
            4
        ) as open_ticket_ratio,

        round(
            total_customer_interactions::float / nullif(total_tickets_updated, 0),
            2
        ) as interactions_per_ticket,

        -- Day-over-day market / billing index movement
        lag(avg_billing_index_usd) over (order by activity_date) as prev_day_billing_index_usd,

        round(
            (
                (avg_billing_index_usd - lag(avg_billing_index_usd) over (order by activity_date))
                / nullif(lag(avg_billing_index_usd) over (order by activity_date), 0)
            ) * 100,
            2
        ) as billing_index_dod_pct_change,

        -- Daily operational load tier for quick triage
        case
            when open_tickets_count >= 5
                 or (total_customer_interactions::float / nullif(total_tickets_updated, 0)) >= 5
                then 'Elevated Operational Load'
            else 'Normal Operational Load'
        end as daily_operational_load_status,

        current_timestamp() as mart_updated_at
    from daily_ops
)

select *
from with_trends