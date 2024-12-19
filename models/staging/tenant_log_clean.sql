select
    id
    , tenant_id
    , creation_date
    , log_type
    , log_details
    , operator_id
    , message_id
    , user_apis
    , json_profile
    , migrate
from {{ ref('tenant_log_seeds') }}
