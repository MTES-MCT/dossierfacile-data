select
    CAST(id as INTEGER)
    , CAST(created_at as TIMESTAMP)
    , CAST(email as VARCHAR)
    , CAST(first_name as VARCHAR) as name
    , CAST(last_login_date as TIMESTAMP) as last_login_at
    , CAST(updated_at as TIMESTAMP)
from {{ source('dossierfacile', 'user_operator') }}
