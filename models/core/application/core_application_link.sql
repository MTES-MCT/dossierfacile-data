with application_link as (
    select
        id
        , application_id
        , token
        , link_type
        , created_at
        , last_sent_at
        , allow_full_access
    from {{ ref('staging_application_link') }}
)

, application_link_log_aggregated as (
    select
        application_link_id
        , MAX(case when log_type = 'APPLICATION_OPENED' then 1 else 0 end) as is_opened
        , MIN(case when log_type = 'APPLICATION_OPENED' then created_at end) as first_opened_at
        , MAX(case when log_type = 'APPLICATION_DOWNLOADED' then 1 else 0 end) as is_downloaded
        , MIN(case when log_type = 'APPLICATION_DOWNLOADED' then created_at end) as first_downloaded_at
    from {{ ref('staging_application_link_log') }}
    where log_type in ('APPLICATION_OPENED', 'APPLICATION_DOWNLOADED')
    group by application_link_id
)

, application_link_status as (
    select
        id
        , application_id
        , token
        , link_type
        , created_at
        , last_sent_at
        , allow_full_access
        , is_opened
        , first_opened_at
        , is_downloaded
        , first_downloaded_at
    from application_link
    left join application_link_log_aggregated
        on application_link.id = application_link_log_aggregated.application_link_id
)

select *
from application_link_status
