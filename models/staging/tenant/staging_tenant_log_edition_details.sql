with json_details as (
    select
        id
        , tenant_id
        , case when log_details ->> 'step' is not null then log_details ->> 'step' when log_details ->> 'newType' is not null then 'Apartement Sharing Type' else 'Document Edition' end as step
        , case when log_details ->> 'editionType' is not null then log_details ->> 'editionType' when log_details ->> 'newType' is not null then 'APPLICATION_TYPE' else 'UPDATE' end as edition_type
        , log_details ->> 'guarantorId' as concerned_guarantor_id
        , log_details ->> 'tenantId' as concerned_tenant_id
        , log_details ->> 'documentCategory' as document_category
        , case when log_details ->> 'documentSubCategory' is not null then log_details ->> 'documentSubCategory' else log_details ->> 'subCategory' end as document_sub_category
        , log_details ->> 'newType' as new_type
        , log_details ->> 'oldType' as old_type
    from {{ source('dossierfacile', 'tenant_log') }}












            {{ filter_recent_data('creation_date') }}
        and log_details is not null

)

, casting_logs as (
    select
        CAST(id as INTEGER)
        , CAST(tenant_id as INTEGER)
        , CAST(step as VARCHAR) as step
        , CAST(edition_type as VARCHAR)
        , CAST(concerned_tenant_id as INTEGER)
        , CAST(concerned_guarantor_id as INTEGER)
        , CAST(document_category as VARCHAR)
        , CAST(document_sub_category as VARCHAR)
        , CAST(old_type as VARCHAR)
        , CAST(new_type as VARCHAR)
    from json_details
)

select
    id
    , tenant_id
    , step
    , edition_type
    , concerned_tenant_id
    , concerned_guarantor_id
    , document_category
    , document_sub_category
    , old_type
    , new_type
from casting_logs
