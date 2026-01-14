select
    {{ dbt_utils.generate_surrogate_key(['sdr.id', 'sdr.denied_option_id']) }} as unique_id
    , sdr.document_denied_at
    , sdr.document_id
    , sdr.denied_option_id
    , sdr.denied_option_value

    -- For denied_reasons recorded before May 12, 2025, contextual information about the document
    -- (document_category, document_sub_category, document_tenant_type) is not available in denied_reasons.
    -- In these cases, we fall back to the denied_options properties using COALESCE.
    -- Note: For generic denied_options, these fields may also be empty, limiting context precision.
    , COALESCE(sdr.document_category, sdo.document_category) as document_category
    , COALESCE(sdr.document_sub_category, sdo.document_sub_category) as document_sub_category
    , sdr.document_category_step
    , COALESCE(sdr.document_tenant_type, sdo.document_tenant_type) as document_tenant_type

    , sdr.operator_comment
from {{ ref('staging_document_denied_reasons') }} as sdr
left join {{ ref('staging_document_denied_options') }} as sdo
    on sdr.denied_option_id = sdo.id
