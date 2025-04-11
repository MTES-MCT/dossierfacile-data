select
    operation.id
    , user_operator.id as operator_id
    , user_operator.email as operator_email
    , user_operator.name as operator_name
    , operation.log_type
    , operation.created_at
    , case when log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end as validation_flag
    , case when log_type = 'ACCOUNT_DENIED' then 1 else 0 end as denial_flag
from {{ ref('staging_tenant_log') }} as operation
left join {{ ref('staging_operator') }} as user_operator
    on operation.operator_id = user_operator.id
where log_type in ('ACCOUNT_DENIED', 'ACCOUNT_VALIDATED')
