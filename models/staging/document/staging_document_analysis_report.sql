with unest_broken_rules as (
    select
        'FAILED' as rule_status
        , CAST(id as INTEGER)
        , CAST(data_document_id as INTEGER) as document_id
        , CAST(analysis_status as VARCHAR) as overall_status
        , JSONB_ARRAY_ELEMENTS(CAST(failed_rules as JSONB)) as rule
        , CAST(comment as VARCHAR) as tenant_comment
        , CAST(created_at as TIMESTAMP) as created_at
    from {{ source('dossierfacile', 'document_analysis_report') }}
)

, unest_passed_rules as (

    select
        'PASSED' as rule_status
        , CAST(id as INTEGER)
        , CAST(data_document_id as INTEGER) as document_id
        , CAST(analysis_status as VARCHAR) as overall_status
        , JSONB_ARRAY_ELEMENTS(CAST(passed_rules as JSONB)) as rule
        , CAST(comment as VARCHAR) as tenant_comment
        , CAST(created_at as TIMESTAMP) as created_at
    from {{ source('dossierfacile', 'document_analysis_report') }}
)

, unest_inconclusive_rules as (

    select
        'INCONCLUSIVE' as rule_status
        , CAST(id as INTEGER)
        , CAST(data_document_id as INTEGER) as document_id
        , CAST(analysis_status as VARCHAR) as overall_status
        , JSONB_ARRAY_ELEMENTS(CAST(inconclusive_rules as JSONB)) as rule
        , CAST(comment as VARCHAR) as tenant_comment
        , CAST(created_at as TIMESTAMP) as created_at
    from {{ source('dossierfacile', 'document_analysis_report') }}
)

, union_all_rules as (
    select
        id
        , document_id
        , overall_status
        , rule_status
        , rule
        , tenant_comment
        , created_at
    from unest_broken_rules

    union all

    select
        id
        , document_id
        , overall_status
        , rule_status
        , rule
        , tenant_comment
        , created_at
    from unest_inconclusive_rules

    union all

    select
        id
        , document_id
        , overall_status
        , rule_status
        , rule
        , tenant_comment
        , created_at
    from unest_passed_rules
)

, flatten_all_rules as (
    select
        id
        , document_id
        , overall_status
        , rule_status
        , tenant_comment
        , created_at
        , rule ->> 'rule' as rule_name
        , rule ->> 'level' as rule_level
        , rule ->> 'message' as rule_message
    from union_all_rules
)

select
    {{ dbt_utils.generate_surrogate_key(['id', 'rule_status', 'rule_name']) }} as unique_id
    , *
from flatten_all_rules
