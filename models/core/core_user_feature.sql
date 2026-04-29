select
    suf.user_id
    , suf.feature_key
    , suf.enabled
    , suf.bucket
    , suf.rollout_pct as assignment_rollout_pct
    , suf.assigned_at
    , suf.assignment_source

    , sff.description as feature_description
    , sff.active as feature_active
    , sff.only_for_new_user
    , sff.rollout_pct as feature_rollout_pct
    , sff.deployment_date
    , sff.created_at as feature_created_at
    , sff.updated_at as feature_updated_at
from {{ ref('staging_user_feature') }} as suf
left join {{ ref('staging_feature_flag') }} as sff
    on suf.feature_key = sff.key
