select
    staging_document_status.document_id
    , staging_document_status.tenant_id
    , staging_document_status.guarantor_id

    , staging_document_status.document_category
    , staging_document_status.document_sub_category
    , case
        when staging_document.id is null then 'UNKNOWN'
        else staging_document.document_category_step
    end as document_category_step

    , COALESCE(staging_document.document_status, staging_document_status.document_status) as document_status

    , staging_document.monthly_net_income

    , COALESCE(staging_document.created_at, staging_document_status.added_at) as created_at
    , staging_document.modified_at
    , staging_document_status.deleted_at

    , staging_document_status.total_add_document_events
    , staging_document_status.total_delete_document_events
from {{ ref('staging_document_status') }} as staging_document_status
left join {{ ref('staging_document') }} as staging_document
    on staging_document_status.document_id = staging_document.id
