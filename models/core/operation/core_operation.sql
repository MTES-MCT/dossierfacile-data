with operator_log as (
    select * from {{ ref('staging_operator_log') }}
    where operator_id is not null
)

select
    operation.id
    , user_operator.id as operator_id
    , user_operator.email as operator_email
    , user_operator.name as operator_name
    , operation.tenant_id
    , operation.log_type
    , operation.created_at
    , operation.processed_documents
    , operation.time_spent
    , case when log_type = 'ACCOUNT_VALIDATION_STOPPED' then 1 else 0 end as operation_flag
    , case when log_type = 'ACCOUNT_VALIDATION_STOPPED' and tenant_status = 'VALIDATED' then 1 else 0 end as validation_flag
    , case when log_type = 'ACCOUNT_VALIDATION_STOPPED' and tenant_status = 'DECLINED' then 1 else 0 end as denial_flag
    , case when log_type = 'ACCOUNT_VALIDATION_STARTED' then 1 else 0 end as validation_started_flag
    , case when log_type = 'ACCOUNT_VALIDATION_STOPPED' then 1 else 0 end as validation_stopped_flag
    , case when log_type = 'APPLICATION_VIEWED' then 1 else 0 end as application_viewed_flag
    , case when log_type = 'APPLICATION_SEARCHED' then 1 else 0 end as application_searched_flag
from operator_log as operation
left join {{ ref('staging_operator') }} as user_operator
    on operation.operator_id = user_operator.id
