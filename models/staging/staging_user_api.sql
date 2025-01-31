select
    CAST(id as INTEGER) as id
    , CAST(url_callback as VARCHAR) as url_callback
    , CAST(name as VARCHAR) as name
    , CAST(name2 as VARCHAR) as name2
    , CAST(site as VARCHAR) as site
    , CAST(partner_api_key_callback as VARCHAR) as partner_api_key_callback
    , CAST(version as INTEGER) as version
    , CAST(disabled as BOOLEAN) as disabled
    , CAST(logo_url as VARCHAR) as logo_url
    , CAST(welcome_url as VARCHAR) as welcome_url
    , CAST(completed_url as VARCHAR) as completed_url
    , CAST(denied_url as VARCHAR) as denied_url
    , CAST(validated_url as VARCHAR) as validated_url
    , CAST(email as VARCHAR) as email
from {{ source('dossierfacile', 'user_api') }}
