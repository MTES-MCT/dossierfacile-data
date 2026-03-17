select
    tenant_origin
    , funnel_type
    , tenant_type
    , status as tenant_status
    , DATE(application_first_shared_at) as application_shared_date
    , SUM(application_is_opened) as nb_account_opened
    , SUM(application_is_opened_full_access) as nb_account_opened_full_access
    , SUM(application_is_downloaded) as nb_account_downloaded
    , SUM(zip_is_downloaded) as nb_account_zip_downloaded
    , SUM(application_is_shared) as nb_account_shared
from {{ ref('core_tenant_account') }}
where application_first_shared_at is not null
group by
    DATE(application_first_shared_at)
    , tenant_origin
    , funnel_type
    , tenant_type
    , status
order by
    DATE(application_first_shared_at) desc
    , tenant_origin
    , funnel_type asc
    , tenant_type asc
    , status asc
