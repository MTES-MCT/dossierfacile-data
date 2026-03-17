with application_link as (
    select
        id
        , application_id
        , token
        , application_link_id
        , link_type
        , created_at
        , expires_at
        , last_sent_at
        , allow_full_access
        , disabled
        , deleted
    from {{ ref('staging_application_link') }}
)

, application_link_log_aggregated as (
    select
        application_link_id
        -- on considère qu'un lien de partage a été ouvert (ou consulté) si :
        -- - le lien de partage a été ouvert (APPLICATION_OPENED)
        -- - un des documents du lien de partage a été téléchargé (WATERMARKED_DOCUMENT_DOWNLOADED)
        , MAX(case when log_type in ('APPLICATION_OPENED', 'WATERMARKED_DOCUMENT_DOWNLOADED') then 1 else 0 end) as is_opened
        , MIN(case when log_type in ('APPLICATION_OPENED', 'WATERMARKED_DOCUMENT_DOWNLOADED') then created_at end) as first_opened_at
        , MAX(case when log_type = 'APPLICATION_PDF_DOWNLOADED' then 1 else 0 end) as is_downloaded
        , MIN(case when log_type = 'APPLICATION_PDF_DOWNLOADED' then created_at end) as first_downloaded_at
    from {{ ref('staging_application_link_log') }}
    where log_type in ('APPLICATION_OPENED', 'APPLICATION_PDF_DOWNLOADED', 'WATERMARKED_DOCUMENT_DOWNLOADED')
    group by application_link_id
)

, application_link_status as (
    select
        id
        , application_id
        , token
        , link_type
        , created_at
        , expires_at
        , last_sent_at
        , allow_full_access
        , disabled
        , deleted
        , is_opened
        , first_opened_at
        , is_downloaded
        , first_downloaded_at
    from application_link
    left join application_link_log_aggregated
        on application_link.application_link_id = application_link_log_aggregated.application_link_id
)

select *
from application_link_status
