select
    CAST(id as INTEGER) as id
    , CAST(tenant_type as VARCHAR) as tenant_type
    , CAST(apartment_sharing_id as INTEGER) as apartment_sharing_id
    , CAST(LEFT(zip_code, 5) as BIGINT) as zip_code -- il y a quelques erreurs dans les saisies de zip_code 
    , CAST(honor_declaration as BOOLEAN) as honor_declaration
    , CAST(last_update_date as TIMESTAMP) as last_updated_at
    , CAST(clarification as VARCHAR) as tenant_comment -- colonne clarification de tenant. c'est le commentaire de l utilisateur à destination du proprietaire
    , CAST(status as VARCHAR) as status
    , CAST(operator_date_time as TIMESTAMP) as last_operation_at
    , CAST(warnings as INTEGER) as warnings
    , CAST(operator_comment as VARCHAR) as operator_comment
    , CAST(abroad as BOOLEAN) as abroad
from {{ source('dossierfacile', 'tenant') }}
