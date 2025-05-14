with raw_application_link_log as (
    select
        CAST(id as INTEGER) as id
        , {{ dbt_utils.generate_surrogate_key(['apartment_sharing_id', 'token']) }} as application_link_id
        , CAST(apartment_sharing_id as INTEGER) as application_id
        , CAST(token as VARCHAR) as token
        , CAST(link_type as VARCHAR) as log_type
        , CAST(creation_date as TIMESTAMP) as created_at
    from {{ source('dossierfacile', 'link_log') }}
    {{ filter_recent_data('creation_date') }}
)

select
    id
    , application_link_id
    , application_id
    , token
    , created_at
    , case
        when log_type = 'FULL_APPLICATION' then 'APPLICATION_OPENED'
        when log_type = 'LIGHT_APPLICATION' then 'APPLICATION_OPENED'
        when log_type = 'DOCUMENT' then 'APPLICATION_DOWNLOADED'
        when log_type = 'DISABLED_LINK' then 'APPLICATION_LINK_DISABLED'
        when log_type = 'ENABLED_LINK' then 'APPLICATION_LINK_ENABLED'
        else log_type
    end as log_type
from raw_application_link_log
