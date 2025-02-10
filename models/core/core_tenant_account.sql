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
        , case when log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end as operation_flag
        , case when log_type = 'ACCOUNT_COMPLETED' then 1 else 0 end as log_completion_flag
    from {{ ref('staging_tenant_log') }}

)

, tenant_status as (
    -- group by tenant_id and get the min/max of the new columns
    select
        tenant_id
        , MIN(creation_date) as creation_date
        , MIN(completion_date) as first_completion_date
        -- on considère que le tenant est complet dès qu'il a été complété, validé ou refusé au moins une fois
        -- cas des couples où l'un des deux conjoints n'a pas de log de completion
        , MAX(completion_flag) as completion_flag
        , MIN(operation_date) as first_operation_date
        , MIN(validation_date) as first_validation_date
        -- on considère que le tenant est validé dès qu'il a été validé au moins une fois
        , MAX(validation_flag) as validation_flag
        , SUM(log_completion_flag) as nb_completions
        , SUM(operation_flag) as nb_traitements
        , SUM(validation_flag) as nb_validations
    from tenant_log_status
    group by
        tenant_id
)

, tenant_status_details as (
    select
        tenant_id
        , creation_date
        , first_completion_date
        , completion_flag
        , first_operation_date
        , first_validation_date
        , validation_flag
        -- , nb_completions
        , nb_traitements
        , nb_validations
        , EXTRACT(epoch from first_completion_date - creation_date) as time_to_complete
        , EXTRACT(epoch from first_validation_date - first_completion_date) as time_to_review
        , case when first_validation_date = first_operation_date then 1 else 0 end as validation_without_denied
    from tenant_status
)

, tenant_partner as (
    select
        staging_tenant_partner_consent.tenant_id
        , MIN(COALESCE(keycloak_client_id, partner_name)) as partner_id
    from {{ ref('staging_tenant_partner_consent') }} as staging_tenant_partner_consent
    left join {{ ref('staging_partner_client') }} as staging_partner_client on staging_tenant_partner_consent.partner_client_id = staging_partner_client.id
    group by tenant_id
)

select
    tenant_status_details.tenant_id as id

    , staging_tenant.apartment_sharing_id
    , staging_tenant.tenant_type
    , staging_tenant.status

    , tenant_partner.partner_id

    , staging_user_account.last_login_at
    , staging_user_account.updated_at
    , staging_user_account.enabled
    , staging_user_account.keycloak_id
    , staging_user_account.france_connect
    , staging_user_account.france_connect_birth_date
    , staging_user_account.france_connect_birth_place
    , staging_user_account.france_connect_birth_country
    , staging_user_account.acquisition_campaign
    , staging_user_account.acquisition_source
    , staging_user_account.acquisition_medium

    , tenant_status_details.creation_date
    , tenant_status_details.first_completion_date
    , tenant_status_details.completion_flag
    , tenant_status_details.first_operation_date
    , tenant_status_details.first_validation_date
    , tenant_status_details.validation_flag
    , tenant_status_details.time_to_complete
    , tenant_status_details.time_to_review
    , tenant_status_details.validation_without_denied
    -- , tenant_status_details.nb_completions
    , tenant_status_details.nb_traitements
    , tenant_status_details.nb_validations

from tenant_status_details
left join {{ ref('staging_user_account') }} as staging_user_account
    on tenant_status_details.tenant_id = staging_user_account.id
left join {{ ref('staging_tenant') }} as staging_tenant
    on tenant_status_details.tenant_id = staging_tenant.id
left join tenant_partner
    on tenant_status_details.tenant_id = tenant_partner.tenant_id
