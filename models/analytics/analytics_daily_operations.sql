select
    DATE(created_at) as operation_date
    , SUM(case when log_type in ('ACCOUNT_DENIED', 'ACCOUNT_VALIDATED') then 1 else 0 end) as nb_operation
    , SUM(case when log_type = 'ACCOUNT_DENIED' then 1 else 0 end) as nb_denied
    , SUM(case when log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end) as nb_validation
from {{ ref('core_tenant_log') }}
group by
    DATE(created_at)
