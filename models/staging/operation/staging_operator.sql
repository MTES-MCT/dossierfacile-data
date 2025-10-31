with raw_operator as (
    select
        CAST(id as INTEGER)
        , CAST(created_at as TIMESTAMP)
        , CAST(email as VARCHAR)
        , CAST(first_name as VARCHAR) as first_name
        , CAST(last_name as VARCHAR) as last_name
        , CAST(last_login_date as TIMESTAMP) as last_login_at
        , CAST(updated_at as TIMESTAMP)
    from {{ source('dossierfacile', 'user_operator') }}
)

select
    id
    , created_at
    , email
    , last_login_at
    , updated_at
    , case
        when (first_name is not null and last_name is not null) then first_name || ' ' || last_name
        when first_name is not null then first_name
        -- parse email address when first_name and last_name are null
        -- jean.dupont+test@example.com -> Jean Dupont
        else INITCAP(SPLIT_PART(SPLIT_PART(email, '@', 1), '.', 1)) || ' ' || INITCAP(SPLIT_PART(SPLIT_PART(SPLIT_PART(email, '@', 1), '.', 2), '+', 1))
    end as name
from raw_operator
