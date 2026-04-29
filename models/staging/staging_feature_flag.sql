select
    CAST("key" as VARCHAR) as key
    , CAST(description as VARCHAR)
    , CAST(active as BOOLEAN)
    , CAST(only_for_new_user as BOOLEAN)
    , CAST(rollout_pct as INTEGER)
    , CAST(deployment_date as TIMESTAMP)
    , CAST(created_at as TIMESTAMP)
    , CAST(updated_at as TIMESTAMP)
from {{ source('dossierfacile', 'feature_flag') }}
