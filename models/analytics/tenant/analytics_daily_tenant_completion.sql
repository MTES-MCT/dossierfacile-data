select
    tenant_origin
    , funnel_type
    , tenant_type
    , status as tenant_status
    , DATE(first_completion_at) as completion_date
    , COALESCE(SUM(is_france_connected::INTEGER), 0) as nb_france_connected
    , SUM(completion_flag) as nb_account_completed
    , SUM(validation_flag) as nb_account_validated
    , SUM(time_to_completion) as total_time_to_completion
    , SUM(time_to_operation) as total_time_to_operation
    , SUM(nb_completions) as total_completions
    , SUM(nb_operations) as total_operations
from {{ ref('core_tenant_account') }}
where first_completion_at is not null
group by
    DATE(first_completion_at)
    , tenant_origin
    , funnel_type
    , tenant_type
    , status
order by
    DATE(first_completion_at) desc
    , tenant_origin
    , funnel_type asc
    , tenant_type asc
    , status asc
