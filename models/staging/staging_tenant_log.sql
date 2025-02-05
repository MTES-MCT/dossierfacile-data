with log_details as (
    select
        id
        , CAST(case
            when log_details ->> 'step' is not null then log_details ->> 'step'
            when log_details ->> 'newType' is not null then 'Apartement Sharing Type'
            else 'Document'
        end as VARCHAR) as step
        , CAST(log_details ->> 'oldType' as VARCHAR) as oldtype
        , CAST(log_details ->> 'newType' as VARCHAR) as newtype
        , CAST(case
            when log_details ->> 'tenantId' is not null then log_details ->> 'tenantId'
            when log_details ->> 'guarantorId' is not null then log_details ->> 'guarantorId'
            else ''
        end as VARCHAR) as user_id_concerned
        , CAST(case
            when log_details ->> 'tenantId' is not null then 'TENANT'
            when log_details ->> 'guarantorId' is not null then 'GUARANTOR'
            else ''
        end as VARCHAR) as user_type_concerned
        , CAST(log_details ->> 'editionType' as VARCHAR) as edition_type
        , CAST(log_details ->> 'documentId' as VARCHAR) as document_id
        , CAST(log_details ->> 'documentCategory' as VARCHAR) as document_category
        , CAST(case when log_details ->> 'documentSubCategory' is not null then log_details ->> 'documentSubCategory' else log_details ->> 'subCategory' end as VARCHAR) as document_sub_category
    from {{ source('dossierfacile', 'tenant_log') }}
    where log_details is not null
    {{ filter_recent_data('creation_date') }}
)

select
    tenant_log.user_apis
    , log_details.step
    , log_details.oldtype
    , log_details.newtype
    , log_details.user_id_concerned
    , log_details.user_type_concerned
    , log_details.edition_type
    , log_details.document_id
    , log_details.document_category
    , log_details.document_sub_category
    , CAST(tenant_log.id as INTEGER) as id

    , CAST(tenant_log.tenant_id as INTEGER) as tenant_id
    , CAST(tenant_log.operator_id as INTEGER) as operator_id
    , CAST(tenant_log.log_type as VARCHAR) as log_type
    , CAST(tenant_log.creation_date as TIMESTAMP) as creation_date
    , CAST(tenant_log.message_id as INTEGER) as message_id
    , CAST(tenant_log.migrate as BOOLEAN) as migrate -- flag migrate dans tenant_log pour indiquer les lignes qui ont été migrées (utile si on veut faire qq chose de plus fin comme par exemple dupliquer le dernier logs de suppression en un log d’archivage et de suppression - il y avait un bug précédemment qui ne permettait pas de distingué la suppression quand un archivage avait eu lieu).
    , CAST(tenant_log.json_profile as JSONB) as json_profile
    , CAST(json_profile ->> 'status' as VARCHAR) as status
    , CAST(LEFT(json_profile ->> 'zipCode', 5) as VARCHAR) as zip_code
from {{ source('dossierfacile', 'tenant_log') }} as tenant_log
left join log_details on tenant_log.id = log_details.id
{{ filter_recent_data('creation_date') }}
