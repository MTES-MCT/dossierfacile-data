select
    tenant_origin
    , tenant_type
    , DATE(created_at) as created_date
    , COUNT(id) as nb_creation
    , COUNT(is_france_connected) as nb_france_connected
    , COUNT(completion_flag) as nb_account_completions
    , COUNT(validation_flag) as nb_account_validations
    , SUM(time_to_completion) as total_time_to_completion
    , SUM(time_to_validation) as total_time_to_validation
    , SUM(nb_completions) as total_completions
    , SUM(nb_operations) as total_operations
    , SUM(nb_validations) as total_validations
from {{ ref('core_tenant_account') }}
group by
    DATE(created_at)
    , tenant_origin
    , tenant_type
order by
    DATE(created_at) desc
    , tenant_origin
    , tenant_type asc
