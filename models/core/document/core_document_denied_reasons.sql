select
    {{ dbt_utils.generate_surrogate_key(['staging_document_denied_reasons.id', 'staging_document_denied_options.code']) }} as unique_id
    , staging_document_denied_reasons.created_at as document_denied_at

    , staging_document_denied_reasons.denied_option_id
    , staging_document_denied_reasons.denied_option_value

    , staging_document_category.document_category
    , staging_document_category.document_sub_category
    , staging_document_denied_options.document_tenant_type
    , staging_document_denied_options.code as denied_option_code
    , staging_document_denied_reasons.operator_comment
from {{ ref('staging_document_denied_reasons') }} as staging_document_denied_reasons
left join {{ ref('staging_document_denied_options') }} as staging_document_denied_options
    on staging_document_denied_reasons.denied_option_id = staging_document_denied_options.id
left join {{ ref('staging_document_category') }} as staging_document_category
    on staging_document_denied_options.document_sub_category = staging_document_category.document_sub_category
where staging_document_category.document_category is not null
