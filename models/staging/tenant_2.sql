select
    tenant_type
    , honor_declaration
    , clarification
    , status
    , operator_comment
    , abroad
    , zip_code
    , CAST(id as INTEGER) as id
    , CAST(apartment_sharing_id as INTEGER) as apartment_sharing_id
    , CAST(last_update_date as TIMESTAMP) as last_update_date
    , CAST(operator_date_time as TIMESTAMP) as operator_date_time
    , CAST(warnings as INTEGER) as warnings
from {{ source('dossierfacile', 'tenant') }}
