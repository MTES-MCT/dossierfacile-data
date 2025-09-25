select
    staging_file.file_id
    , staging_file.document_id
    , staging_file.created_at
    , staging_file.page_number
    , core_document.document_category
    , core_document.document_sub_category
    , core_document.document_category_step
    , core_document.document_status
    , core_document.tenant_id
    , core_document.guarantor_id
from {{ ref('staging_file') }} as staging_file
left join {{ ref('core_document') }} as core_document
    on staging_file.document_id = core_document.id
