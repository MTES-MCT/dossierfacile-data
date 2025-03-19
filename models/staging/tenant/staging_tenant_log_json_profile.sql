with json_details as (
    select
        log_type
        , json_profile ->> 'id' as tenant_id
        , json_profile ->> 'status' as status
        , json_profile ->> 'zipCode' as zip_code
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'documents') ->> 'id' as document_id
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'documents') ->> 'documentCategory' as document_category
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'documents') ->> 'subCategory' as document_sub_category
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'documents') ->> 'documentStatus' as document_status
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'guarantors') ->> 'id' as guarantor_id
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(json_profile -> 'guarantors') -> 'documents') ->> 'id' as guarantor_document_id
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(json_profile -> 'guarantors') -> 'documents') ->> 'documentCategory' as guarantor_document_category
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(json_profile -> 'guarantors') -> 'documents') ->> 'subCategory' as guarantor_document_sub_category
        , JSONB_ARRAY_ELEMENTS(JSONB_ARRAY_ELEMENTS(json_profile -> 'guarantors') -> 'documents') ->> 'documentStatus' as guarantor_document_status
    from {{ source('dossierfacile', 'tenant_log') }}






            {{ filter_recent_data('creation_date') }}
        and json_profile is not null

)

, casting_logs as (
    select
        CAST(log_type as VARCHAR)
        , CAST(tenant_id as INTEGER)
        , CAST(status as VARCHAR)
        , CAST(zip_code as VARCHAR)
        , CAST(document_id as INTEGER)
        , CAST(document_category as VARCHAR)
        , CAST(document_sub_category as VARCHAR)
        , CAST(document_status as VARCHAR)
        , CAST(guarantor_id as INTEGER)
        , CAST(guarantor_document_id as INTEGER)
        , CAST(guarantor_document_category as VARCHAR)
        , CAST(guarantor_document_sub_category as VARCHAR)
        , CAST(guarantor_document_status as VARCHAR)
    from json_details
)

select
    tenant_id
    , log_type
    , status
    , zip_code
    , document_id
    , document_category
    , document_sub_category
    , document_status
    , guarantor_id
    , guarantor_document_id
    , guarantor_document_category
    , guarantor_document_sub_category
    , guarantor_document_status
from casting_logs
