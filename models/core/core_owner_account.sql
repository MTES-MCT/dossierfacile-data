with owner_property_status as (
    select
        staging_user_account.id
        , COUNT(distinct staging_property.id) as nb_property_created
        , SUM(case when staging_property.displayed then 1 else 0 end) as nb_property_displayed
        , SUM(case when staging_property.validated then 1 else 0 end) as nb_property_validated
        , MIN(staging_property.created_at) as first_property_created_at
        , MIN(staging_property.validated_date) as first_property_validation_date
        , AVG(case when validated then staging_property.count_visit end) as avg_count_visit_validated
        , AVG(case when validated then staging_property.rent_cost end) as avg_rent_cost_validated
        , AVG(case when validated then staging_property.charges_cost end) as avg_charges_cost_validated
    from {{ ref('staging_user_account') }} as staging_user_account
    inner join {{ ref('staging_property') }} as staging_property on staging_user_account.id = staging_property.owner_id
    where user_type = 'OWNER'
    group by staging_user_account.id
)

select
    staging_user_account.id
    , staging_user_account.created_at
    , staging_user_account.last_login_at
    , owner_property_status.nb_property_created
    , owner_property_status.nb_property_displayed
    , owner_property_status.nb_property_validated
    , owner_property_status.first_property_created_at
    , owner_property_status.first_property_validation_date
    , owner_property_status.avg_count_visit_validated
    , owner_property_status.avg_rent_cost_validated
    , owner_property_status.avg_charges_cost_validated
from {{ ref('staging_user_account') }} as staging_user_account
left join owner_property_status on staging_user_account.id = owner_property_status.id
