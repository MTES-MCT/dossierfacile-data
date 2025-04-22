with tenant_log as (
    select * from {{ ref('staging_tenant_log') }}
    where log_type in ('ACCOUNT_DENIED', 'ACCOUNT_VALIDATED')
)

, operator_log as (
    select * from {{ ref('staging_operator_log') }}
    where log_type in ('ACCOUNT_VALIDATION_STARTED', 'ACCOUNT_VALIDATION_STOPPED')
)

, operation as (
    select
        {{ dbt_utils.generate_surrogate_key(['id', 'log_type']) }} as operation_id
        , tenant_id
        , operator_id
        , log_type
        , created_at
        , null as processed_documents
        , null as time_spent
    from tenant_log

    union all

    select
        {{ dbt_utils.generate_surrogate_key(['id', 'log_type']) }} as operation_id
        , tenant_id
        , operator_id
        , log_type
        , created_at
        , processed_documents
        , time_spent
    from operator_log
)

select
    operation.operation_id as id
    , user_operator.id as operator_id
    , user_operator.email as operator_email
    , user_operator.name as operator_name
    , operation.log_type
    , operation.created_at
    , operation.processed_documents
    , operation.time_spent
    , case when log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end as operation_flag
    , case when log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end as validation_flag
    , case when log_type = 'ACCOUNT_DENIED' then 1 else 0 end as denial_flag
    , case when log_type = 'ACCOUNT_VALIDATION_STARTED' then 1 else 0 end as validation_started_flag
    , case when log_type = 'ACCOUNT_VALIDATION_STOPPED' then 1 else 0 end as validation_stopped_flag
from operation
left join {{ ref('staging_operator') }} as user_operator
    on operation.operator_id = user_operator.id
