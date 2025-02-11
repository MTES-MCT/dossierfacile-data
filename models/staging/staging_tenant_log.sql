with log_details as (
    select
        id
        , case
            when log_details ->> 'step' is not null then log_details ->> 'step'
            when log_details ->> 'newType' is not null then 'Apartement Sharing Type'
            else 'Document'
        end as step
        , log_details ->> 'oldType' as oldtype
        , log_details ->> 'newType' as newtype
        , case
            when log_details ->> 'tenantId' is not null then log_details ->> 'tenantId'
            when log_details ->> 'guarantorId' is not null then log_details ->> 'guarantorId'
            else ''
        end as user_id_concerned
        , case
            when log_details ->> 'tenantId' is not null then 'TENANT'
            when log_details ->> 'guarantorId' is not null then 'GUARANTOR'
            else ''
        end as user_type_concerned
        , log_details ->> 'editionType' as edition_type
        , log_details ->> 'documentId' as document_id
        , log_details ->> 'documentCategory' as document_category
        , case when log_details ->> 'documentSubCategory' is not null then log_details ->> 'documentSubCategory' else log_details ->> 'subCategory' end as document_sub_category
    from {{ source('dossierfacile', 'tenant_log') }}
    where log_details is not null
)

, casting_log_details as (
    select
        CAST(id as INTEGER) as id
        , CAST(step as VARCHAR) as step
        , CAST(oldtype as VARCHAR) as oldtype
        , CAST(newtype as VARCHAR) as newtype
        , CAST(user_id_concerned as VARCHAR) as user_id_concerned
        , CAST(user_type_concerned as VARCHAR) as user_type_concerned
        , CAST(edition_type as VARCHAR) as edition_type
        , CAST(document_id as VARCHAR) as document_id
        , CAST(document_category as VARCHAR) as document_category
        , CAST(document_sub_category as VARCHAR) as document_sub_category
    from log_details
)

, casting_log as (
    select 
        CAST(id as INTEGER) as id
        , CAST(user_apis as BIGINT []) as user_apis
        , CAST(tenant_id as INTEGER) as tenant_id
        , CAST(operator_id as INTEGER) as operator_id
        , CAST(log_type as VARCHAR) as log_type
        , CAST(creation_date as TIMESTAMP) as creation_date
        , CAST(message_id as INTEGER) as message_id
        , CAST(migrate as BOOLEAN) as migrate -- flag migrate dans tenant_log pour indiquer les lignes qui ont été migrées (utile si on veut faire qq chose de plus fin comme par exemple dupliquer le dernier logs de suppression en un log d’archivage et de suppression - il y avait un bug précédemment qui ne permettait pas de distingué la suppression quand un archivage avait eu lieu).
        , CAST(json_profile as JSONB) as json_profile
        , CAST(json_profile ->> 'status' as VARCHAR) as status
        , CAST(LEFT(tenant_log.json_profile ->> 'zipCode', 5) as VARCHAR) as zip_code
    from {{ source('dossierfacile', 'tenant_log') }} as tenant_log
)
select

    casting_log.id
    , casting_log.user_apis
    , casting_log.tenant_id
    , casting_log.operator_id
    , casting_log.log_type
    , casting_log.creation_date
    , casting_log.message_id
    , casting_log.migrate
    , casting_log.json_profile
    , casting_log.status

    , casting_log_details.step
    , casting_log_details.oldtype
    , casting_log_details.newtype
    , casting_log_details.user_id_concerned
    , casting_log_details.user_type_concerned
    , casting_log_details.edition_type
    , casting_log_details.document_id
    , casting_log_details.document_category
    , casting_log_details.document_sub_category

from casting_log
left join casting_log_details on casting_log.id = casting_log_details.id
{{ filter_recent_data('creation_date') }}
