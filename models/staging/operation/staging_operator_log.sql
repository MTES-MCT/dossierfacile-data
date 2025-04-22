select
    CAST(id as INTEGER)
    , CAST(tenant_id as INTEGER)
    , CAST(operator_id as INTEGER)
    , case
        when action_type = 'START_PROCESS' then 'ACCOUNT_VALIDATION_STARTED'
        when action_type = 'STOP_PROCESS' then 'ACCOUNT_VALIDATION_STOPPED'
    end as log_type
    , CAST(creation_date as TIMESTAMP) as created_at

    , CAST(tenant_status as VARCHAR)
    , CAST(processed_documents as INTEGER)
    , CAST(time_spent as INTEGER)
from {{ source('dossierfacile', 'operator_log') }}
{{ filter_recent_data('creation_date') }}
