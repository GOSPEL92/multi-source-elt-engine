with source as (
    select * from {{ source('raw_saas_data', 'raw_market_rates') }}
),

cleaned as (
    select
        id as asset_id,
        upper(symbol) as asset_symbol,
        name as asset_name,
        coalesce(current_price, current_price__v_double)::float as price_usd,
        market_cap::float as market_cap_usd,
        total_volume::float as trading_volume_24h_usd,
        price_change_percentage_24h::float as price_change_pct_24h,
        _extracted_at::timestamp_tz as extracted_at,
        date_trunc('day', _extracted_at::timestamp_tz)::date as extracted_date
    from source
    where coalesce(current_price, current_price__v_double) is not null
)

select * from cleaned