select
    staging_document_denied_options.id
    , staging_document_denied_reasons.checked_options
    , staging_document_denied_reasons.checked_options_id
    , staging_document_denied_reasons.comment
    , staging_document_denied_reasons.message_id
    , staging_document_denied_reasons.message_data
    , staging_document_denied_reasons.created_at as document_denied_at
    , staging_document_denied_options.message_value
    , staging_document_denied_options.document_user_type
    , staging_document_denied_options.code
    , staging_document.id as document_id
    , core_document_reference.document_category
    , staging_document_denied_options.document_sub_category
    , staging_document.created_at as document_created_at
    , staging_document.tenant_id
    , staging_document.guarantor_id
    , core_tenant_account.tenant_origin
    , case when staging_document.tenant_id is not null then 'TENANT' else 'GUARANTOR' end as user_type
from {{ ref('staging_document_denied_options') }} as staging_document_denied_options
left join {{ ref('staging_document_denied_reasons') }} as staging_document_denied_reasons on staging_document_denied_options.id = staging_document_denied_reasons.checked_options_id
left join {{ ref('core_document_reference') }} as core_document_reference on staging_document_denied_options.document_sub_category = core_document_reference.document_sub_category
left join {{ ref('staging_document') }} as staging_document on staging_document_denied_reasons.document_id = staging_document.id
left join {{ ref('core_tenant_account') }} as core_tenant_account on staging_document.tenant_id = core_tenant_account.id

/*
with document_user as (
    select distinct
        staging_document.id as document_id
        , staging_document.document_category
        , staging_document.document_sub_category
        , tenant_id
        , guarantor_id
        , case when staging_document.tenant_id is not null then 'TENANT' else 'GUARANTOR' end as user_type
    from {{ ref('staging_document') }}
)

select
    document_user.document_id
    , document_user.document_category
    , document_user.document_sub_category
    , document_user.tenant_id
    , document_user.guarantor_id
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
from {{ ref('staging_document_denied_reasons') }} as staging_document_denied_reasons
inner join {{ ref('staging_document_denied_options') }} as staging_document_denied_options on staging_document_denied_options.id = staging_document_denied_reasons.checked_options_id
left join document_user on document_user.document_id = staging_document_denied_reasons.document_id
left join {{ ref('core_tenant_account') }} as core_tenant_account on document_user.tenant_id = core_tenant_account.id
*/
