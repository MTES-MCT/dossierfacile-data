select
    CAST(id as INTEGER)
    , CAST(tenant_type as VARCHAR)
    , CAST(apartment_sharing_id as INTEGER)
    , case
        when LEFT(zip_code, 5) ~ '^[0-9]{5}$' then LEFT(zip_code, 5)
        else 'invalid'
    end as zip_code
    , CAST(honor_declaration as BOOLEAN)
    , CAST(last_update_date as TIMESTAMP) as last_updated_at
    , CAST(COALESCE(status, 'INCOMPLETE') as VARCHAR) as status
    , CAST(operator_date_time as TIMESTAMP) as last_operation_at
    , CAST(warnings as INTEGER) as deletion_warnings
    , CAST(abroad as BOOLEAN) as tenant_abroad
from {{ source('dossierfacile', 'tenant') }}
{{ filter_recent_data('last_update_date') }}
