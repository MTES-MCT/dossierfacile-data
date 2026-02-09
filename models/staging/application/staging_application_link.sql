-- this table contains all the links. both email, partner and owner link
select
    CAST(id as INTEGER) as id
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(expiration_date as TIMESTAMP) as expires_at
    , CAST(apartment_sharing_id as INTEGER) as application_id
    , CAST(token as VARCHAR) as token
    , {{ dbt_utils.generate_surrogate_key(['apartment_sharing_id', 'token']) }} as application_link_id
    , CAST(full_data as BOOLEAN) as allow_full_access
    , CAST(disabled as BOOLEAN) as disabled
    , CAST(deleted as BOOLEAN) as deleted
    , CAST(link_type as VARCHAR) as link_type -- LINK, MAIL, OWNER, PARTNER
    , CAST(last_sent_datetime as TIMESTAMP) as last_sent_at
from {{ source('dossierfacile', 'apartment_sharing_link') }}
{{ filter_recent_data('creation_date') }}
