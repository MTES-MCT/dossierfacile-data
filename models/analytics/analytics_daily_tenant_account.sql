select
    tenant_origin
    , tenant_type
    , status as tenant_status
    , DATE(created_at) as created_date
    , COUNT(id) as nb_creation
    , SUM(is_france_connected::INTEGER) as nb_france_connected
    , SUM(completion_flag) as nb_account_completions
    , SUM(validation_flag) as nb_account_validations
    , SUM(validation_at_first_operation) as nb_validation_at_first_operation
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
    , status
order by
    DATE(created_at) desc
    , tenant_origin
    , tenant_type asc
    , status asc
