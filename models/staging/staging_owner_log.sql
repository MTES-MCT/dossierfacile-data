select
    CAST(id as INTEGER)
    , CAST(owner_id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(log_type as VARCHAR)                 -- la table staging_owner_log_json met a plat les data des json de suppression des comptes
from {{ source('dossierfacile', 'owner_log') }}
