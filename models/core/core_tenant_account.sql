with tenant_dates as ( -- récupération des dates pour chaque tenant
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
)

select
    staging_tenant_log.tenant_id as id
    , apartment_sharing_id
    , last_login_date
    , update_date_time
    , provider
    , provider_id
    , enabled
    , keycloak_id
    , france_connect
    , france_connect_sub
    , france_connect_birth_date
    , france_connect_birth_place
    , france_connect_birth_country
    , staging_tenant.status
    , tenant_dates.creation_date
    , completion_date
    , completion_flag
    , firstoperation_date
    , validation_date
    , validation_flag
from {{ ref('staging_tenant_log') }} as staging_tenant_log
inner join tenant_dates on staging_tenant_log.tenant_id = tenant_dates.tenant_id
left join {{ ref('staging_user_account') }} on staging_tenant_log.tenant_id = {{ ref('staging_user_account') }}.id
left join {{ ref('staging_tenant') }} on staging_tenant_log.tenant_id = {{ ref('staging_tenant') }}.id
