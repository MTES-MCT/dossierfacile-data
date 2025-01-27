select
    staging_tenant_log.tenant_id
    , MIN(case when staging_tenant_log.log_type in ('ACCOUNT_CREATED', 'ACCOUNT_CREATED_VIA_KC', 'FC_ACCOUNT_CREATION') then staging_tenant_log.creation_date end) as creation_date
    , MIN(case when staging_tenant_log.log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then staging_tenant_log.creation_date end) as completion_date
    , MAX(case when staging_tenant_log.log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end) as completion_flag
    , MIN(case when staging_tenant_log.log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then staging_tenant_log.creation_date end) as firstoperation_date
    , MIN(case when staging_tenant_log.log_type = 'ACCOUNT_VALIDATED' then staging_tenant_log.creation_date end) as validation_date
    , MAX(case when staging_tenant_log.log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end) as validation_flag
from {{ ref('staging_tenant_log') }} as staging_tenant_log
group by
    staging_tenant_log.tenant_id
