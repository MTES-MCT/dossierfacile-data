select
    staging_tenant_log.tenant_id as id
    , apartment_sharing_id
    , last_login_date
    , update_date_time
    , provider
    , provider_id
    , enabled
    , keycloak_id
    , france_connect
    , france_connect_sub
    , france_connect_birth_date
    , france_connect_birth_place
    , france_connect_birth_country
    , staging_tenant.status
    , core_tenant_dates.creation_date
    , completion_date
    , completion_flag
    , firstoperation_date
    , validation_date
    , validation_flag
from {{ ref('staging_tenant_log') }} as staging_tenant_log
inner join {{ ref('core_tenant_dates') }} on core_tenant_dates.tenant_id = staging_tenant_log.tenant_id
left join {{ ref('staging_user_account') }} on staging_tenant_log.tenant_id = {{ ref('staging_user_account') }}.id
left join {{ ref('staging_tenant') }} on staging_tenant_log.tenant_id = {{ ref('staging_tenant') }}.id
