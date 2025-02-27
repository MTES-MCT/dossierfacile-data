select
    DATE_TRUNC('week', created_at) as created_date
    , COUNT(distinct case when log_type in ('ACCOUNT_DENIED', 'ACCOUNT_VALIDATED') then tenant_id end) as nb_operation
    , COUNT(distinct case when log_type = 'ACCOUNT_DENIED' then tenant_id end) as nb_denied
    , COUNT(distinct case when log_type = 'ACCOUNT_VALIDATED' then tenant_id end) as nb_validation
from {{ ref('core_tenant_log') }}
group by
    DATE_TRUNC('week', created_at)
