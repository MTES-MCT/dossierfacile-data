select
    {{ source ('dbt_dev', 'tenant_log_2') }}.tenant_id as id
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
    , {{ source ('dbt_dev', 'tenant_2') }}.status
    , MIN(case when log_type in ('ACCOUNT_CREATED', 'ACCOUNT_CREATED_VIA_KC', 'FC_ACCOUNT_CREATION') then {{ source ('dbt_dev', 'tenant_log_2') }}.creation_date end) as creation_date
    , MIN(case when log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then {{ source ('dbt_dev', 'tenant_log_2') }}.creation_date end) as completion_date
    , MAX(case when log_type in ('ACCOUNT_COMPLETED', 'ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then 1 else 0 end) as completion_flag
    , MIN(case when log_type in ('ACCOUNT_VALIDATED', 'ACCOUNT_DENIED') then {{ source ('dbt_dev', 'tenant_log_2') }}.creation_date end) as firstoperation_date
    , MIN(case when log_type = 'ACCOUNT_VALIDATED' then {{ source ('dbt_dev', 'tenant_log_2') }}.creation_date end) as validation_date
    , MAX(case when log_type = 'ACCOUNT_VALIDATED' then 1 else 0 end) as validation_flag
from {{ source('dbt_dev', 'tenant_log_2') }}
left join {{ source ('dbt_dev', 'user_account_2') }} on {{ source ('dbt_dev', 'tenant_log_2') }}.tenant_id = {{ source ('dbt_dev', 'user_account_2') }}.id
left join {{ source ('dbt_dev', 'tenant_2') }} on {{ source ('dbt_dev', 'tenant_log_2') }}.tenant_id = {{ source ('dbt_dev', 'tenant_2') }}.id
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
