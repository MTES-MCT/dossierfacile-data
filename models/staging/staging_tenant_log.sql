select
    creation_date
    , log_type
    , user_apis
    , json_profile
    , migrate
    , CAST(id as INTEGER) as id
    , CAST(tenant_id as INTEGER) as tenant_id
    , CAST(log_details ->> 'tenantId' as INTEGER) as tenant_id_concerned
    , CAST(log_details ->> 'guarantorId' as INTEGER) as guarantor_id_concerned
    , log_details ->> 'editionType' as edition_type
    , log_details ->> 'documentCategory' as document_category
    , case
        when log_details ->> 'documentSubCategory' is not null
            then log_details ->> 'documentSubCategory'
        else log_details ->> 'subCategory'
    end as document_sub_category
    , CAST(operator_id as INTEGER) as operator_id
    , CAST(message_id as INTEGER) as message_id
    , json_profile ->> 'status' as status
    , json_profile ->> 'zipCode' as zip_code
from {{ source('dossierfacile', 'tenant_log') }}
{{ filter_recent_data('creation_date') }}
