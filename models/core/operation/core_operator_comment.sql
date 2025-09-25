select
    id
    , tenant_id
    , created_at
    , operator_comment
    , case
        when LOWER(operator_comment) ~ '.*(soupçon|suspicion|suspect|modif|doute|contrefait|faux|fausse|falsifi|fraud).*' then 1
        else 0
    end as fraud_suspicion_flag
from {{ ref('staging_tenant_log') }}
where log_type = 'OPERATOR_COMMENT'
