with source as (
    select *
    from {{ source('raw_saas_data', 'raw_support_tickets') }}
    where id is not null
),

deduped as (
    select *
    from source
    qualify row_number() over (
        partition by id
        order by
            try_to_timestamp_ntz(updated_at::varchar) desc nulls last,
            created_at desc nulls last
    ) = 1
),

transformed as (
    select
        id::varchar as ticket_id,
        account_id::varchar as account_id,
        ticket_number::bigint as ticket_number,
        coalesce(nullif(trim(title::varchar), ''), 'Untitled Ticket') as ticket_title,
        coalesce(nullif(trim(state::varchar), ''), 'open') as ticket_status,
        created_at,
        updated_at,
        closed_at,
        coalesce(try_to_number(comments_count::varchar), 0)::bigint as comments_count,
        author_login::varchar as customer_handle,
        to_date(try_to_timestamp_ntz(updated_at::varchar)) as activity_date,
        case
            when coalesce(try_to_number(comments_count::varchar), 0) = 0 then 'No Interactions'
            when coalesce(try_to_number(comments_count::varchar), 0) <= 2 then 'Low Engagement'
            when coalesce(try_to_number(comments_count::varchar), 0) <= 5 then 'Medium Engagement'
            else 'High Engagement'
        end as interaction_volume_category
    from deduped
)

select *
from transformed