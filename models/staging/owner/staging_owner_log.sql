{{ config(enabled=false) }}

select
    CAST(id as INTEGER)
    , CAST(owner_id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(log_type as VARCHAR)
from {{ source('dossierfacile', 'owner_log') }}
