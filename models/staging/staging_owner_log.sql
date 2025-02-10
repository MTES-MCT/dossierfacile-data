select
    CAST(id as INTEGER) as id
    , CAST(owner_id as INTEGER) as owner_id
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(log_type as VARCHAR) as log_type                 -- la table staging_owner_log_json met a plat les data des json de suppression des comptes
from owner_log
