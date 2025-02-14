with owner_property_status as (
    select
        owner_id
        , COUNT(distinct id) as nb_property_created
        , SUM(case when displayed then 1 else 0 end) as nb_property_displayed
        , SUM(case when validated then 1 else 0 end) as nb_property_validated
        , MIN(created_at) as first_property_created_at
        , MIN(validated_date) as first_property_validation_date
        , AVG(case when validated then count_visit end) as avg_count_visit_validated
    from {{ ref('staging_property') }}
    group by owner_id
)

select
    staging_user_account.id
    , staging_user_account.created_at
    , staging_user_account.last_login_at
    , staging_user_account.updated_at
    , staging_user_account.enabled
    , staging_user_account.keycloak_id
    , staging_user_account.is_france_connected

    , owner_property_status.nb_property_created
    , owner_property_status.nb_property_displayed
    , owner_property_status.nb_property_validated
    , owner_property_status.first_property_created_at
    , owner_property_status.first_property_validation_date
    , owner_property_status.avg_count_visit_validated
from {{ ref('staging_user_account') }} as staging_user_account
left join owner_property_status
    on staging_user_account.id = owner_property_status.owner_id
where staging_user_account.user_type = 'OWNER'
