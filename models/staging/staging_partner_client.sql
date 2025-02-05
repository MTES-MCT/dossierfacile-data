select
    CAST(id as INTEGER) as id
    , CAST(url_callback as VARCHAR) as url_callback
    , CAST(name as VARCHAR) as keycloak_client_id
    , CAST(name2 as VARCHAR) as partner_name
    , CAST(site as VARCHAR) as site
    , CAST(disabled as BOOLEAN) as disabled
    , CAST(email as VARCHAR) as email
from {{ source('dossierfacile', 'user_api') }}
