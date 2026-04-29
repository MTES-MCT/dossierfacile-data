select
    CAST(user_id as BIGINT)
    , CAST(feature_key as VARCHAR)
    , CAST(enabled as BOOLEAN)
    , CAST(bucket as INTEGER)
    , CAST(rollout_pct as INTEGER)
    , CAST(assigned_at as TIMESTAMP)
    , CAST(assignment_source as VARCHAR)
from {{ source('dossierfacile', 'user_feature_assignment') }}
{{ filter_recent_data('assigned_at') }}
