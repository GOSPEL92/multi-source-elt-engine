with tickets as (
    select *
    from {{ ref('stg_support_tickets') }}
    where ticket_id is not null
),

snapshot_history as (
    select
        ticket_id,
        count(*) as snapshot_record_count,
        count(distinct ticket_status) as distinct_statuses_seen,
        min(dbt_valid_from) as first_snapshotted_at,
        max(dbt_valid_from) as latest_snapshot_change_at,
        max(case when dbt_valid_to is null then 1 else 0 end) as is_currently_tracked_in_snapshot
    from {{ ref('snap_support_tickets') }}
    group by ticket_id
),

enriched as (
    select
        t.ticket_id,
        t.ticket_number,
        t.ticket_title,
        t.ticket_status,
        t.customer_handle,
        t.interaction_volume_category,
        t.comments_count,
        t.activity_date,
        try_to_timestamp_ntz(t.created_at::varchar) as created_at_ts,
        try_to_timestamp_ntz(t.updated_at::varchar) as updated_at_ts,
        try_to_timestamp_ntz(t.closed_at::varchar) as closed_at_ts,
        coalesce(s.snapshot_record_count, 0) as snapshot_record_count,
        coalesce(s.distinct_statuses_seen, 0) as distinct_statuses_seen,
        s.first_snapshotted_at,
        s.latest_snapshot_change_at,
        coalesce(s.is_currently_tracked_in_snapshot, 0) as is_currently_tracked_in_snapshot,

        case
            when lower(t.ticket_status) = 'closed'
                 and try_to_timestamp_ntz(t.closed_at::varchar) is not null
                 and try_to_timestamp_ntz(t.created_at::varchar) is not null
                then greatest(
                    0,
                    datediff(
                        'day',
                        try_to_timestamp_ntz(t.created_at::varchar),
                        try_to_timestamp_ntz(t.closed_at::varchar)
                    )
                )
            when try_to_timestamp_ntz(t.created_at::varchar) is not null
                then greatest(
                    0,
                    datediff(
                        'day',
                        try_to_timestamp_ntz(t.created_at::varchar),
                        current_timestamp()
                    )
                )
            else null
        end as ticket_age_days,

        case
            when lower(t.ticket_status) <> 'closed'
                 and (
                     t.interaction_volume_category in ('High Engagement', 'Medium Engagement')
                     or datediff('day', try_to_timestamp_ntz(t.created_at::varchar), current_timestamp()) >= 7
                 )
                then 'High Escalation Risk'
            when lower(t.ticket_status) <> 'closed'
                then 'Watchlist - Open'
            else 'Resolved / Closed'
        end as escalation_risk_tier,

        current_timestamp() as dbt_updated_at
    from tickets t
    left join snapshot_history s
        on t.ticket_id = s.ticket_id
)

select *
from enriched