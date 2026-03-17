select
    tenant_origin
    , funnel_type
    , tenant_type
    , status as tenant_status
    , DATE(first_validation_at) as validation_date
    , COALESCE(SUM(is_france_connected::INTEGER), 0) as nb_france_connected
    , SUM(validation_flag) as nb_account_validated
    , SUM(validation_at_first_operation) as nb_validation_at_first_operation
    , SUM(time_to_validation) as total_time_to_validation
    , SUM(time_to_operation) as total_time_to_operation
    , SUM(nb_operations) as total_operations
    , SUM(nb_validations) as total_validations
    , SUM(application_is_opened) as nb_account_opened
    , SUM(application_is_opened_full_access) as nb_account_opened_full_access
    , SUM(application_is_downloaded) as nb_account_downloaded
    , SUM(zip_is_downloaded) as nb_account_zip_downloaded
    , SUM(application_is_shared) as nb_account_shared
from {{ ref('core_tenant_account') }}
where first_validation_at is not null
group by
    DATE(first_validation_at)
    , tenant_origin
    , funnel_type
    , tenant_type
    , status
order by
    DATE(first_validation_at) desc
    , tenant_origin
    , funnel_type asc
    , tenant_type asc
    , status asc
