select
    operator_id
    , operator_name
    , operator_email
    , DATE_TRUNC('hour', created_at) as operation_hour
    , SUM(operation_flag) as nb_operation
    , SUM(denial_flag) as nb_denied
    , SUM(validation_flag) as nb_validation
    , SUM(validation_started_flag) as nb_validation_started
    , SUM(validation_stopped_flag) as nb_validation_stopped
    , SUM(processed_documents) as nb_processed_documents
    , SUM(time_spent) as total_time_spent
from {{ ref('core_operation') }}
group by
    DATE_TRUNC('hour', created_at)
    , operator_id
    , operator_name
    , operator_email
