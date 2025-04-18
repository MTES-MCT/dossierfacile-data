select
    CAST(id as INTEGER)
    , CAST(name as VARCHAR) as keycloak_client_id
    , CAST(name2 as VARCHAR) as partner_name
    , CAST(disabled as BOOLEAN)
    , CAST(url_callback as VARCHAR)
    , CAST(site as VARCHAR)
    , CAST(email as VARCHAR)
from {{ source('dossierfacile', 'user_api') }}
