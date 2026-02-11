select
    tenant_origin
    , funnel_type
    , tenant_type
    , status as tenant_status
    , DATE(application_first_opened_at) as application_opening_date
    , COALESCE(SUM(is_france_connected::INTEGER), 0) as nb_france_connected
    , SUM(application_is_opened) as nb_account_opened
    , SUM(application_is_opened_full_access) as nb_account_opened_full_access
    , SUM(application_is_downloaded) as nb_account_downloaded
    , SUM(application_is_shared) as nb_account_shared
from {{ ref('core_tenant_account') }}
where application_first_opened_at is not null
group by
    DATE(application_first_opened_at)
    , tenant_origin
    , funnel_type
    , tenant_type
    , status
order by
    DATE(application_first_opened_at) desc
    , tenant_origin
    , funnel_type asc
    , tenant_type asc
    , status asc
