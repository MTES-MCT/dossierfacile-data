select
    staging_document.id as document_id
    , staging_document.document_category
    , staging_document.document_sub_category
    , staging_document.document_category_step
    , staging_document.created_at as document_created_at
    , staging_document_denied_reasons.created_at as document_denied_at

    , staging_document_denied_reasons.denied_options_id
    , staging_document_denied_options.denied_options_value

    , staging_document_denied_options.document_sub_category as denied_options_sub_category
    , staging_document_denied_options.code
    , staging_document_denied_reasons.operator_comment
    , staging_document.tenant_id

    , staging_document.guarantor_id
    , case
        when staging_document.tenant_id is not null then 'TENANT'
        else 'GUARANTOR'
    end as document_tenant_type

    --, core_tenant_account.tenant_origin

from {{ ref('staging_document_denied_reasons') }} as staging_document_denied_reasons
left join {{ ref('staging_document') }} as staging_document
    on staging_document_denied_reasons.document_id = staging_document.id
left join {{ ref('staging_document_denied_options') }} as staging_document_denied_options
    on staging_document_denied_reasons.denied_options_id = staging_document_denied_options.id
-- left join {{ ref('core_tenant_account') }} as core_tenant_account
--     on staging_document.tenant_id = core_tenant_account.id
