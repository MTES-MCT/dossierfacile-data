select
    CAST(tenant_id as INTEGER)
    , CAST(userapi_id as INTEGER) as partner_client_id
    , CAST(COALESCE(access_granted_date, '1970-01-01 00:00:00') as TIMESTAMP) as access_granted_at
from {{ source('dossierfacile', 'tenant_userapi') }}
{{ filter_recent_data('access_granted_date') }}
