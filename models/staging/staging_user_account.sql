select
    CAST(id as INTEGER) as id
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(last_login_date as TIMESTAMP) as last_login_at
    , CAST(update_date_time as TIMESTAMP) as updated_at
    , CAST(enabled as BOOLEAN) as enabled -- information de keycloak
    , CAST(keycloak_id as VARCHAR) as keycloak_id
    , CAST(france_connect as BOOLEAN) as france_connect
    -- , CAST(france_connect_sub as VARCHAR) as france_connect_sub
    , CAST(case when france_connect_birth_date = 'birthdate' then null else france_connect_birth_date end as TIMESTAMP) as france_connect_birth_date
    , CAST(case when france_connect_birth_place = 'birthplace' then null else france_connect_birth_place end as VARCHAR) as france_connect_birth_place
    , CAST(case when france_connect_birth_country = 'birthcountry' then null else france_connect_birth_country end as VARCHAR) as france_connect_birth_country
    , CAST(user_type as VARCHAR) as user_type
    , CAST(acquisition_campaign as VARCHAR) as acquisition_campaign
    , CAST(acquisition_source as VARCHAR) as acquisition_source
    , CAST(acquisition_medium as VARCHAR) as acquisition_medium
from {{ source('dossierfacile', 'user_account') }}
{{ filter_recent_data('creation_date') }}
