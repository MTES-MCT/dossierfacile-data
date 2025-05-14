-- this table only contains email links
select
    CAST(id as INTEGER) as id
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(apartment_sharing_id as INTEGER) as application_id
    , CAST(token as VARCHAR) as token
    , CAST(full_data as BOOLEAN) as allow_full_access
    , CAST(disabled as BOOLEAN) as disabled
    , CAST(link_type as VARCHAR) as link_type
    , CAST(last_sent_datetime as TIMESTAMP) as last_sent_at
from {{ source('dossierfacile', 'apartment_sharing_link') }}
{{ filter_recent_data('creation_date') }}
-- for now, we only want to get email links and this is the only type of link in the table
and link_type = 'MAIL'
