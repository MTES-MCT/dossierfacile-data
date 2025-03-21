select
    core_tenant_account.tenant_origin
    , DATE_TRUNC('MONTH', core_tenant_log.created_at) as created_date
    , SUM(case when core_tenant_log.log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end) as nb_validations
    , SUM(case when core_tenant_log.log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end) as nb_operations
from {{ ref('core_tenant_account') }} as core_tenant_account
inner join {{ ref('core_tenant_log') }} as core_tenant_log on core_tenant_account.id = core_tenant_log.tenant_id
where
    tenant_origin ilike '%jinka%'
    and log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED')
group by
    DATE_TRUNC('MONTH', core_tenant_log.created_at)
    , core_tenant_account.tenant_origin
