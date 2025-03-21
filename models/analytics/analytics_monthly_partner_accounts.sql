select
    DATE_TRUNC('month', created_at) as created_at
    , tenant_origin
    , COUNT(*) as nb_creations
    , SUM(completion_flag) as nb_completions
    , SUM(validation_flag) as nb_validations
from {{ ref('core_tenant_account') }}
where tenant_origin in ('hybrid-pap', 'dfconnect-locservice', 'dfconnect-jinka', 'dfconnect-immojeune')
group by
    DATE_TRUNC('month', created_at)
    , tenant_origin
