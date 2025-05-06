select
    id
    , tenant_id
    , type_guarantor
from {{ source('dossierfacile', 'guarantor') }}
