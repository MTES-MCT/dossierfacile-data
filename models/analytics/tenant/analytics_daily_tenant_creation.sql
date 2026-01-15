select
    tenant_origin
    , funnel_type
    , tenant_type
    , status as tenant_status
    , DATE(created_at) as creation_date
    , COUNT(id) as nb_account_created
    , COALESCE(SUM(is_france_connected::INTEGER), 0) as nb_france_connected
    , SUM(completion_flag) as nb_account_completed
    , SUM(validation_flag) as nb_account_validated
from {{ ref('core_tenant_account') }}
where created_at is not null
group by
    DATE(created_at)
    , tenant_origin
    , funnel_type
    , tenant_type
    , status
order by
    DATE(created_at) desc
    , tenant_origin
    , funnel_type asc
    , tenant_type asc
    , status asc
