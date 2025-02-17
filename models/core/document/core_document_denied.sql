with document_user as (
    select distinct
        staging_document.id as document_id
        , staging_document.document_category
        , staging_document.document_sub_category
        , case when staging_document.tenant_id is not null then tenant_id else guarantor_id end as user_id
        , case when staging_document.tenant_id is not null then 'TENANT' else 'GUARANTOR' end as user_type
    from {{ ref('staging_document') }}
)

select
    document_user.document_id
    , document_user.document_category
    , document_user.document_sub_category
    , document_user.user_id
    , document_user.user_type
    , core_tenant_account.tenant_origin
    , staging_document_denied_reasons.id
    , staging_document_denied_reasons.checked_options
    , staging_document_denied_reasons.checked_options_id
    , staging_document_denied_reasons.comment
    , staging_document_denied_reasons.message_id
    , staging_document_denied_reasons.message_data
    , staging_document_denied_reasons.created_at
    , staging_document_denied_options.message_value
    , staging_document_denied_options.document_user_type
    , staging_document_denied_options.code
from document_user
inner join {{ ref('staging_document_denied_reasons') }} as staging_document_denied_reasons on document_user.document_id = staging_document_denied_reasons.document_id
inner join {{ ref('staging_document_denied_options') }} as staging_document_denied_options on staging_document_denied_options.id = staging_document_denied_reasons.checked_options_id
inner join {{ ref('core_tenant_account') }} as core_tenant_account on document_user.user_id = core_tenant_account.id
