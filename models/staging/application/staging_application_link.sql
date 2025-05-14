-- this table contains all the links, both email and link
-- to have both email and link in the same table, we use the token and token_public in the application table
select
    {{ dbt_utils.generate_surrogate_key(['id', 'token']) }} as id
    , id as application_id
    , token
    , 'LINK' as link_type
    , null::TIMESTAMP as created_at
    , null::TIMESTAMP as last_sent_at
    , true as allow_full_access
from {{ ref('staging_application') }}
where token is not null

union all

select
    {{ dbt_utils.generate_surrogate_key(['id', 'token_public']) }} as id
    , id as application_id
    , token_public as token
    , 'LINK' as link_type
    , null::TIMESTAMP as created_at
    , null::TIMESTAMP as last_sent_at
    , false as allow_full_access
from {{ ref('staging_application') }}
where token_public is not null

union all

select
    {{ dbt_utils.generate_surrogate_key(['application_id', 'token']) }} as id
    , application_id
    , token
    , case
        when link_type = 'MAIL' and allow_full_access = true then 'EMAIL'
        when link_type = 'MAIL' and allow_full_access = false then 'EMAIL'
    end as link_type
    , created_at
    , last_sent_at
    , allow_full_access
from {{ ref('staging_application_email_link') }}
