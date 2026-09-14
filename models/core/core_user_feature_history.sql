{{ config(
    materialized = 'table',
    unique_key = ['id'],
    indexes=[
      {'columns': ['user_id', 'feature_key', 'changed_at']}
    ]
) }}

select
    sufh.id
    , sufh.user_id
    , sufh.feature_key
    , sufh.enabled
    , sufh.bucket
    , sufh.rollout_pct
    , sufh.changed_at
    , sufh.reason

    , sff.description as feature_description
    , sff.only_for_new_user
    , sff.rollout_pct as feature_rollout_pct
    , sff.deployment_date
    , sff.created_at as feature_created_at
    , sff.updated_at as feature_updated_at
from {{ ref('staging_user_feature_history') }} as sufh
left join {{ ref('staging_feature_flag') }} as sff
    on sufh.feature_key = sff.key
