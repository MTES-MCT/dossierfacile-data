select
    CAST(id as BIGINT)
    , CAST(user_id as BIGINT)
    , CAST(feature_key as VARCHAR)
    , CAST(enabled as BOOLEAN)
    , CAST(bucket as INTEGER)
    , CAST(rollout_pct as INTEGER)
    , CAST(changed_at as TIMESTAMP)
    , CAST(reason as VARCHAR)
from {{ source('dossierfacile', 'user_feature_assignment_history') }}
{{ filter_recent_data('changed_at') }}
