with operations_with_first_validation_date as (
    select
        id
        , tenant_id
        , created_at
        , MIN(case when log_type = 'ACCOUNT_VALIDATED' then created_at end)
            over (partition by tenant_id)
        as first_validation_at
    from {{ ref('core_operation') }}
    where operation_flag = 1
)

, operations_count_per_tenant as (
    select
        tenant_id
        , COUNT(id) as nb_operations_before_first_validation
    from operations_with_first_validation_date
    where
        created_at <= first_validation_at
        and first_validation_at is not null
    group by tenant_id
)

, validated_tenants_with_operations as (
    select
        cta.created_at
        , cta.funnel_type
        , cta.tenant_origin
        , cta.status
        , ocpt.tenant_id
        , cta.id
        , ocpt.nb_operations_before_first_validation
    from operations_count_per_tenant as ocpt
    left join {{ ref('core_tenant_account') }} as cta
        on ocpt.tenant_id = cta.id
)

select
    funnel_type
    , tenant_origin
    , status
    , DATE_TRUNC('week', created_at) as created_week
    , COUNT(tenant_id) as nb_validated_accounts
    , SUM(nb_operations_before_first_validation) as nb_operations_before_first_validation
from validated_tenants_with_operations
group by
    DATE_TRUNC('week', created_at)
    , funnel_type
    , tenant_origin
    , status
order by DATE_TRUNC('week', created_at) desc
