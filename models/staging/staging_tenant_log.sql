-- with tenant_log_details as (
--     select
--         id
--         , case
--             when log_details ->> 'step' = 'Names' then 'TenantIdentityChanged'
--             when log_details ->> 'newType' is not null then 'ApplicationTypeChanged' 
--             when log_details ->> 'step' is null then NULL 
--         else 'DocumentChanged'
--         end as step
--         , log_details ->> 'oldType' as old_application_type
--         , log_details ->> 'newType' as new_application_type
--         , case
--             when log_details ->> 'tenantId' is not null then log_details ->> 'tenantId'
--             when log_details ->> 'guarantorId' is not null then log_details ->> 'guarantorId'
--             else ''
--         end as edited_user_id
--         , case
--             when log_details ->> 'tenantId' is not null then 'TENANT'
--             when log_details ->> 'guarantorId' is not null then 'GUARANTOR'
--             else ''
--         end as edited_user_type
--         , log_details ->> 'editionType' as edition_type
--         , log_details ->> 'documentId' as document_id
--         , log_details ->> 'documentCategory' as document_category
--         , case
--             when log_details ->> 'documentSubCategory' is not null then log_details ->> 'documentSubCategory'
--             else log_details ->> 'subCategory'
--         end as document_sub_category
--     from {{ source('dossierfacile', 'tenant_log') }}
--     where log_details is not null
-- )

with casting_log as (
    select 
        CAST(id as INTEGER)
        , CAST(tenant_id as INTEGER)
        , CAST(operator_id as INTEGER)
        , CAST(log_type as VARCHAR)
        , CAST(creation_date as TIMESTAMP) as created_at
        -- flag migrate dans tenant_log pour indiquer les lignes qui ont été migrées 
        -- utile si on veut faire qq chose de plus fin comme par exemple dupliquer le dernier logs de suppression en un log d’archivage et de suppression
        -- il y avait un bug précédemment qui ne permettait pas de distinguer la suppression quand un archivage avait eu lieu.
        , CAST(migrate as BOOLEAN)
    from {{ source('dossierfacile', 'tenant_log') }} as tenant_log
    {{ filter_recent_data('creation_date') }}
)
select

    casting_log.id
    , casting_log.tenant_id
    , casting_log.operator_id
    , casting_log.log_type
    , casting_log.created_at
    , casting_log.migrate
from casting_log

