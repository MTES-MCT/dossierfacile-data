select
    tenant_log.id
    , tenant_log.tenant_id
    , tenant_log.operator_id
    , operator.email as operator_email
    , operator.name as operator_name
    , tenant_log.created_at
    -- DEPRECATED: operator_comment is no longer stored in the database
    , '[REDACTED]' as operator_comment
    , 0 as fraud_suspicion_flag
from {{ ref('staging_tenant_log') }} as tenant_log
left join {{ ref('staging_operator') }} as operator
    on tenant_log.operator_id = operator.id
where log_type = 'OPERATOR_COMMENT'
