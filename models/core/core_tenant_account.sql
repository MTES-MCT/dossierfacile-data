with tenant_log_status as (
    -- add new columns based on log_type
    select
        tenant_id
        , case when log_type in ('ACCOUNT_CREATED', 'ACCOUNT_CREATED_VIA_KC', 'FC_ACCOUNT_CREATION') then creation_date end as creation_date
        , case when log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then creation_date end as completion_date
        , case when log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end as completion_flag
        , case when log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then creation_date end as operation_date
        , case when log_type = 'ACCOUNT_VALIDATED' then creation_date end as validation_date
        , case when log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end as validation_flag
    from {{ ref('staging_tenant_log') }}

)

, tenant_status as (
    -- group by tenant_id and get the min/max of the new columns
    select
        tenant_id
        , MIN(creation_date) as creation_date
        , MIN(completion_date) as first_completion_date
        -- on considère que le tenant est complet dès qu'il a été complété, validé ou refusé au moins une fois ?
        , MAX(completion_flag) as completion_flag
        , MIN(operation_date) as first_operation_date
        , MIN(validation_date) as first_validation_date
        -- on considère que le tenant est validé dès qu'il a été validé au moins une fois ?
        , MAX(validation_flag) as validation_flag
    from tenant_log_status
    group by
        tenant_id
)

select
    tenant_status.tenant_id as id

    , staging_tenant.apartment_sharing_id
    , staging_tenant.tenant_type
    , staging_tenant.status

    , staging_user_account.last_login_date
    , staging_user_account.update_date_time
    , staging_user_account.provider
    , staging_user_account.provider_id
    , staging_user_account.enabled
    , staging_user_account.keycloak_id
    , staging_user_account.france_connect
    , staging_user_account.france_connect_sub
    , staging_user_account.france_connect_birth_date
    , staging_user_account.france_connect_birth_place
    , staging_user_account.france_connect_birth_country

    , tenant_status.creation_date
    , tenant_status.first_completion_date
    , tenant_status.completion_flag
    , tenant_status.first_operation_date
    , tenant_status.first_validation_date
    , tenant_status.validation_flag
from tenant_status
left join {{ ref('staging_user_account') }} as staging_user_account
    on tenant_status.tenant_id = staging_user_account.id
left join {{ ref('staging_tenant') }} as staging_tenant
    on tenant_status.tenant_id = staging_tenant.id
