with application_link_aggregated as (
    select
        application_id
        , MAX(is_opened) as is_opened
        , MAX(case when allow_full_access = true then is_opened end) as is_opened_full_access
        , MIN(first_opened_at) as first_opened_at
        , MAX(is_downloaded) as is_downloaded
        , MIN(first_downloaded_at) as first_downloaded_at
    from {{ ref('core_application_link') }}
    group by application_id
)

, application as (
    select
        id
        , application_type
        , application_pdf_status
        , application_pdf_file_id
        , updated_at
    from {{ ref('staging_application') }}
)

select
    application.id
    , application.application_type
    , application.application_pdf_status
    , application.application_pdf_file_id
    , application.updated_at

    , application_link_aggregated.is_opened
    , application_link_aggregated.is_opened_full_access
    , application_link_aggregated.first_opened_at
    , application_link_aggregated.is_downloaded
    , application_link_aggregated.first_downloaded_at
from application
left join application_link_aggregated
    on application.id = application_link_aggregated.application_id
