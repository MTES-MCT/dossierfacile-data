select
    CAST(id as INTEGER)
    , CAST(owner_id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(count_visit as INTEGER)
    , CAST(displayed as BOOLEAN)
    , CAST(validated as BOOLEAN)
    , CAST(type as VARCHAR)
    , CAST(validated_date as TIMESTAMP)
    , CAST(dpe_not_required as BOOLEAN)
from {{ source('dossierfacile', 'property') }}
