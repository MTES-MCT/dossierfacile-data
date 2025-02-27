select 
date_trunc('month', created_at) as created_at
, COUNT(*) as nb_creations
, SUM(completion_flag) as nb_completions
, SUM(validation_flag) as nb_validations
from {{ ref('core_tenant_account') }}
where tenant_origin in ('hybrid-pap', 'dfconnect-locservice', 'dfconnect-jinka', 'dfconnect-immojeune')
group by
date_trunc('month', created_at)