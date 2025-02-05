select
    CAST(tenant_id as INTEGER) as tenant_id
    , CAST(userapi_id as INTEGER) as partner_client_id
    , CAST(access_granted_date as TIMESTAMP) as access_granted_date
from {{ source('dossierfacile', 'tenant_userapi') }}
{{ filter_recent_data('access_granted_date') }}
