with daily_denial_reasons as (
    select
        denied_option_id
        , denied_option_value
        , document_category
        , document_sub_category
        , document_category_step
        , document_tenant_type
        , DATE(document_denied_at) as denial_date
        , COUNT(unique_id) as nb_denial_reasons
    from {{ ref('core_document_denied_reasons') }}
    group by
        DATE(document_denied_at)
        , denied_option_id
        , denied_option_value
        , document_category
        , document_sub_category
        , document_category_step
        , document_tenant_type
)

, daily_operations as (
    select
        DATE(operation_hour) as operation_date
        , SUM(nb_operation) as nb_operation
    from {{ ref('analytics_hourly_operations') }}
    group by
        DATE(operation_hour)
)

select
    daily_denial_reasons.*
    , daily_operations.nb_operation
from daily_denial_reasons
left join daily_operations
    on daily_denial_reasons.denial_date = daily_operations.operation_date
