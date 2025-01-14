select
    provider
    , provider_id
    , enabled
    , keycloak_id
    , france_connect
    , france_connect_sub
    , france_connect_birth_place
    , france_connect_birth_country
    , preferred_name
    , user_type
    , acquisition_campaign
    , acquisition_source
    , acquisition_medium
    , CAST(id as INTEGER) as id
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(last_login_date as TIMESTAMP) as last_login_date
    , CAST(update_date_time as TIMESTAMP) as update_date_time
    , CAST(france_connect_birth_date as TIMESTAMP) as france_connect_birth_date
from {{ source('dossierfacile', 'user_account') }}
where france_connect_birth_date <> 'birthdate'
