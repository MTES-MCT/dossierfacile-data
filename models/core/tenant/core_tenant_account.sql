with tenant_log_status as (
    -- add new columns based on log_type
    select
        tenant_id
        , case when log_type in ('ACCOUNT_CREATED', 'ACCOUNT_CREATED_VIA_KC', 'FC_ACCOUNT_CREATION', 'FC_ACCOUNT_LINK', 'ACCOUNT_EDITED') then created_at end as created_at
        , case when log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then created_at end as completion_at
        , case when log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end as completion_flag
        , case when log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then created_at end as operation_at
        , case when log_type = 'ACCOUNT_VALIDATED' then created_at end as validation_at
        , case when log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end as validation_flag
        , case when log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end as operation_flag
        , case when log_type = 'ACCOUNT_COMPLETED' then 1 else 0 end as user_completion_flag
    from {{ ref('staging_tenant_log') }}
)

, tenant_status as (
    -- group by tenant_id and get the min/max of the new columns
    select
        tenant_id
        , MIN(created_at) as created_at
        , MIN(completion_at) as first_completion_at
        -- on considère que le tenant est complet dès qu'il a été complété, validé ou refusé au moins une fois
        -- cas des couples où l'un des deux conjoints n'a pas de log de completion
        , MAX(completion_flag) as completion_flag
        , MIN(operation_at) as first_operation_at
        , MIN(validation_at) as first_validation_at
        -- on considère que le tenant est validé dès qu'il a été validé au moins une fois
        , MAX(validation_flag) as validation_flag
        , SUM(user_completion_flag) as nb_completions
        , SUM(operation_flag) as nb_operations
        , SUM(validation_flag) as nb_validations
    from tenant_log_status
    group by
        tenant_id
)

, tenant_status_details as (
    select
        tenant_id
        , created_at
        , first_completion_at
        , completion_flag
        , first_operation_at
        , first_validation_at
        , validation_flag
        , nb_completions
        , nb_operations
        , nb_validations
        , CAST(EXTRACT(epoch from first_completion_at - created_at) as INTEGER) as time_to_completion
        , CAST(EXTRACT(epoch from first_operation_at - first_completion_at) as INTEGER) as time_to_operation
        , CAST(EXTRACT(epoch from first_validation_at - first_completion_at) as INTEGER) as time_to_validation
        , case when first_validation_at = first_operation_at then 1 else 0 end as validation_at_first_operation
    from tenant_status
)

, tenant_partner_consent_list as (
    select distinct
        tenant_id
        , ARRAY_AGG(keycloak_client_id) over (
            partition by tenant_id
            order by access_granted_at asc
            rows between unbounded preceding and unbounded following
        ) as partner_consent_list
        , FIRST_VALUE(access_granted_at) over (
            partition by tenant_id
            order by access_granted_at asc
        ) as first_access_granted_at
        , FIRST_VALUE(keycloak_client_id) over (
            partition by tenant_id
            order by access_granted_at asc
        ) as first_partner_consent
    from {{ ref('staging_tenant_partner_consent') }} as staging_tenant_partner_consent
    left join {{ ref('staging_partner_api_client') }} as staging_partner_client
        on staging_tenant_partner_consent.partner_client_id = staging_partner_client.id
)

, tenant_account_data as (
    select
        tenant_status_details.tenant_id as id

        , staging_tenant.application_id
        , staging_tenant.tenant_type
        , staging_tenant.status
        , staging_tenant.zip_code
        , staging_tenant.honor_declaration
        , staging_tenant.operator_comment
        , staging_tenant.beneficiary_type

        , staging_user_account.last_login_at
        , staging_user_account.updated_at
        , staging_user_account.enabled
        , staging_user_account.keycloak_id
        , staging_user_account.is_france_connected
        , staging_user_account.acquisition_campaign

        , tenant_status_details.created_at
        , tenant_status_details.first_completion_at
        , tenant_status_details.completion_flag
        , tenant_status_details.first_operation_at
        , tenant_status_details.first_validation_at
        , tenant_status_details.validation_flag
        , tenant_status_details.time_to_completion
        , tenant_status_details.time_to_operation
        , tenant_status_details.time_to_validation
        , tenant_status_details.validation_at_first_operation
        , tenant_status_details.nb_completions
        , tenant_status_details.nb_operations
        , tenant_status_details.nb_validations

        , tenant_partner_consent_list.partner_consent_list
        , tenant_partner_consent_list.first_access_granted_at

        -- Détermine l'origine du dossier locataire:
        -- - Si une campagne d'acquisition est définie, on utilise le préfixe 'link-' suivi du nom de la campagne
        -- - Si le premier consentement partenaire a été donné dans l'heure suivant la création du compte
        --   et que ce n'est pas dfconnect-proprietaire, on utilise l'identifiant du partenaire
        -- - Sinon, on considère que c'est un dossier créé directement sur DossierFacile
        , case
            when staging_user_account.acquisition_campaign is not null then 'link-' || staging_user_account.acquisition_campaign
            when
                tenant_partner_consent_list.first_access_granted_at < tenant_status_details.created_at + INTERVAL '1 hour'
                and tenant_partner_consent_list.first_partner_consent <> 'dfconnect-proprietaire' then tenant_partner_consent_list.first_partner_consent
            else 'organic-dossierfacile'
        end as tenant_origin
    from tenant_status_details
    left join {{ ref('staging_user_account') }} as staging_user_account
        on tenant_status_details.tenant_id = staging_user_account.id
    left join {{ ref('staging_tenant') }} as staging_tenant
        on tenant_status_details.tenant_id = staging_tenant.id
    left join tenant_partner_consent_list as tenant_partner_consent_list
        on tenant_status_details.tenant_id = tenant_partner_consent_list.tenant_id
    where staging_user_account.user_type = 'TENANT'
)

select
    tenant_account_data.*

    , core_application.application_type
    , core_application.application_pdf_status
    , core_application.is_opened as application_is_opened
    , core_application.is_opened_full_access as application_is_opened_full_access
    , core_application.first_opened_at as application_first_opened_at
    , core_application.is_downloaded as application_is_downloaded
    , core_application.first_downloaded_at as application_first_downloaded_at

    , staging_tenant_document.identification_last_sub_category

    , staging_tenant_document.identification_first_added_at
    , staging_tenant_document.has_identification_document
    , staging_tenant_document.financial_last_sub_category

    , staging_tenant_document.financial_first_added_at
    , staging_tenant_document.has_financial_document
    , staging_tenant_document.residency_last_sub_category

    , staging_tenant_document.residency_first_added_at
    , staging_tenant_document.has_residency_document
    , staging_tenant_document.professional_last_sub_category

    , staging_tenant_document.professional_first_added_at
    , staging_tenant_document.has_professional_document
    , staging_tenant_document.tax_last_sub_category

    , staging_tenant_document.tax_first_added_at
    , staging_tenant_document.has_tax_document
    , staging_tenant_document.document_completion_flag

    -- Un dossier locataire est considéré comme partagé si une des conditions suivantes est vérifiée:
    -- - il est validé et a été partagé via une intégration partenaire au moins une fois
    -- - le dossier de candidature associé a été ouvert au moins une fois
    -- - le dossier de candidature associé a été téléchargé au moins une fois
    , case
        when
            tenant_account_data.validation_flag = 1
            and tenant_account_data.partner_consent_list is not null
            and ARRAY_LENGTH(tenant_account_data.partner_consent_list, 1) > 0
            then 1
        when core_application.is_opened = 1 then 1
        when core_application.is_downloaded = 1 then 1
        else 0
    end as application_is_shared

    , case
        when tenant_origin like 'hybrid-%' then 'api'
        else 'dossierfacile'
    end as funnel_type
from tenant_account_data
left join {{ ref('core_application') }} as core_application
    on tenant_account_data.application_id = core_application.id
left join {{ ref('staging_tenant_document') }} as staging_tenant_document
    on
        tenant_account_data.id = staging_tenant_document.tenant_id
        and staging_tenant_document.tenant_type = 'TENANT'
