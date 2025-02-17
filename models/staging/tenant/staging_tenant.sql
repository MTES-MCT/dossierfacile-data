select
    CAST(id as INTEGER)
    , CAST(tenant_type as VARCHAR)
    , CAST(apartment_sharing_id as INTEGER)
    , CAST(LEFT(zip_code, 5) as BIGINT) as zip_code
    , CAST(honor_declaration as BOOLEAN)
    , CAST(last_update_date as TIMESTAMP) as last_updated_at
    , CAST(clarification as VARCHAR) as tenant_comment -- colonne clarification de tenant. c'est le commentaire de l utilisateur à destination du proprietaire
    , CAST(COALESCE(status, 'INCOMPLETE') as VARCHAR)
    , CAST(operator_date_time as TIMESTAMP) as last_operation_at
    , CAST(warnings as INTEGER) as deletion_warnings
    , CAST(operator_comment as VARCHAR)
    , CAST(abroad as BOOLEAN) as tenant_abroad
from {{ source('dossierfacile', 'tenant') }}
{{ filter_recent_data('last_update_date') }}
