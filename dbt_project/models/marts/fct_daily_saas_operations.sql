{{
    config(
        materialized='incremental',
        unique_key='activity_date',
        incremental_strategy='merge'
    )
}}

with daily_tickets as (
    select
        activity_date,
        count(distinct ticket_id) as total_tickets_updated,
        count(distinct case when lower(ticket_status) = 'open' then ticket_id end) as open_tickets_count,
        count(distinct case when lower(ticket_status) = 'closed' then ticket_id end) as closed_tickets_count,
        coalesce(sum(comments_count), 0) as total_customer_interactions
    from {{ ref('stg_support_tickets') }}
    where activity_date is not null
    {% if is_incremental() %}
        and activity_date >= (
            select coalesce(dateadd(day, -3, max(activity_date)), '1900-01-01'::date)
            from {{ this }}
        )
    {% endif %}
    group by activity_date
),

latest_market_index as (
    select
        extracted_date,
        avg(price_usd) as avg_billing_index_usd,
        sum(trading_volume_24h_usd) as total_market_liquidity_usd
    from {{ ref('stg_market_rates') }}
    where extracted_date is not null
    group by extracted_date
),

final as (
    select
        t.activity_date,
        coalesce(t.total_tickets_updated, 0) as total_tickets_updated,
        coalesce(t.open_tickets_count, 0) as open_tickets_count,
        coalesce(t.closed_tickets_count, 0) as closed_tickets_count,
        coalesce(t.total_customer_interactions, 0) as total_customer_interactions,
        coalesce(m.avg_billing_index_usd, 0) as avg_billing_index_usd,
        coalesce(m.total_market_liquidity_usd, 0) as total_market_liquidity_usd,
        current_timestamp() as dbt_updated_at
    from daily_tickets t
    left join latest_market_index m
        on t.activity_date = m.extracted_date
    where t.activity_date is not null
    qualify row_number() over (partition by t.activity_date order by t.activity_date) = 1
)

select * from final