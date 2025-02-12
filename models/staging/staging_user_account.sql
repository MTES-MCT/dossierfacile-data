select
    CAST(id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(last_login_date as TIMESTAMP) as last_login_at
    , CAST(update_date_time as TIMESTAMP) as updated_at
    , CAST(enabled as BOOLEAN) -- information de keycloak
    , CAST(keycloak_id as VARCHAR)
    , CAST(france_connect as BOOLEAN) as is_france_connected
    , CAST(case 
        when france_connect_birth_date = 'birthdate' then null 
        else france_connect_birth_date 
    end as TIMESTAMP) as france_connect_birth_date
    , CAST(case
        when france_connect_birth_place = 'birthplace' then null 
        else france_connect_birth_place 
    end as VARCHAR) as france_connect_birth_place
    , CAST(case 
        when france_connect_birth_country = 'birthcountry' then null 
        else france_connect_birth_country 
    end as VARCHAR) as france_connect_birth_country
    , CAST(user_type as VARCHAR)
    , CAST(acquisition_campaign as VARCHAR)
    , CAST(acquisition_source as VARCHAR)
    , CAST(acquisition_medium as VARCHAR)
from {{ source('dossierfacile', 'user_account') }}
{{ filter_recent_data('creation_date') }}
