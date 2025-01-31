select
    user_apis
    , json_profile
    , log_details
    , CAST(id as INTEGER) as id
    , CAST(tenant_id as INTEGER) as tenant_id
    , CAST(operator_id as INTEGER) as operator_id
    , CAST(log_type as VARCHAR) as log_type
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(message_id as INTEGER) as message_id
    , CAST(migrate as BOOLEAN) as migrate
    , CAST(log_details ->> 'tenantId' as INTEGER) as tenant_id_concerned
    , CAST(log_details ->> 'guarantorId' as INTEGER) as guarantor_id_concerned
    , CAST(log_details ->> 'editionType' as VARCHAR) as edition_type
    , CAST(log_details ->> 'documentCategory' as VARCHAR) as document_category
    , CAST(case
        when log_details ->> 'documentSubCategory' is not null
            then log_details ->> 'documentSubCategory'
        else log_details ->> 'subCategory'
    end as VARCHAR) as document_sub_category
    , CAST(json_profile ->> 'status' as VARCHAR) as status
    , CAST(LEFT(json_profile ->> 'zipCode', 5) as VARCHAR) as zip_code
from {{ source('dossierfacile', 'tenant_log') }}
{{ filter_recent_data('creation_date') }}
