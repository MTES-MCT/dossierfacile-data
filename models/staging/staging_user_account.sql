select
    CAST(id as INTEGER) as id
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(email as VARCHAR) as email
    , CAST(first_name as VARCHAR) as first_name
    , CAST(last_name as VARCHAR) as last_name
    , CAST(last_login_date as TIMESTAMP) as last_login_date
    , CAST(update_date_time as TIMESTAMP) as update_date_time
    , CAST(image_url as VARCHAR) as image_url
    , CAST(provider as VARCHAR) as provider
    , CAST(provider_id as VARCHAR) as provider_id
    , CAST(enabled as BOOLEAN) as enabled
    , CAST(keycloak_id as VARCHAR) as keycloak_id
    , CAST(france_connect as BOOLEAN) as france_connect
    , CAST(france_connect_sub as VARCHAR) as france_connect_sub
    , CAST(case when france_connect_birth_date = 'birthdate' then null else france_connect_birth_date end as TIMESTAMP) as france_connect_birth_date
    , CAST(france_connect_birth_place as VARCHAR) as france_connect_birth_place
    , CAST(france_connect_birth_country as VARCHAR) as france_connect_birth_country
    , CAST(preferred_name as VARCHAR) as preferred_name
    , CAST(user_type as VARCHAR) as user_type
    , CAST(acquisition_campaign as VARCHAR) as acquisition_campaign
    , CAST(acquisition_source as VARCHAR) as acquisition_source
    , CAST(acquisition_medium as VARCHAR) as acquisition_medium
from {{ source('dossierfacile', 'user_account') }}
{{ filter_recent_data('creation_date') }}