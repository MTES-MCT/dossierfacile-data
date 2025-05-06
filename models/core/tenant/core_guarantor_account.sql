select
    staging_guarantor.id
    , staging_guarantor.type_guarantor
    , staging_guarantor.tenant_id

    , staging_tenant_document.identification_last_sub_category
    , staging_tenant_document.identification_first_added_at
    , staging_tenant_document.has_identification_document

    , staging_tenant_document.financial_last_sub_category
    , staging_tenant_document.financial_first_added_at
    , staging_tenant_document.has_financial_document

    , staging_tenant_document.residency_last_sub_category
    , staging_tenant_document.residency_first_added_at
    , staging_tenant_document.has_residency_document

    , staging_tenant_document.professional_last_sub_category
    , staging_tenant_document.professional_first_added_at
    , staging_tenant_document.has_professional_document

    , staging_tenant_document.tax_last_sub_category
    , staging_tenant_document.tax_first_added_at
    , staging_tenant_document.has_tax_document

    , (
        staging_tenant_document.has_identification_document
        + staging_tenant_document.has_financial_document
        + staging_tenant_document.has_residency_document
        + staging_tenant_document.has_professional_document
        + staging_tenant_document.has_tax_document
    ) = 5 as completion_flag
from {{ ref('staging_guarantor') }} as staging_guarantor
left join {{ ref('staging_tenant_document') }} as staging_tenant_document
    on
        staging_guarantor.id = staging_tenant_document.guarantor_id
        and staging_tenant_document.tenant_type = 'GUARANTOR'
