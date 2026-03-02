select
    tenant_log.id
    , tenant_log.tenant_id
    , tenant_log.operator_id
    , operator.email as operator_email
    , operator.name as operator_name
    , tenant_log.created_at
    , tenant_log.operator_comment
    , case
        when LOWER(operator_comment) ~ '.*(soupçon|suspicion|suspect|modif|doute|contrefait|faux|fausse|falsifi|fraud).*' then 1
        else 0
    end as fraud_suspicion_flag
from {{ ref('staging_tenant_log') }} as tenant_log
left join {{ ref('staging_operator') }} as operator
    on tenant_log.operator_id = operator.id
where log_type = 'OPERATOR_COMMENT'
