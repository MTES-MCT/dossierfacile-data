select
    CAST(id as INTEGER)
    , CAST(document_category as VARCHAR)
    , CAST(tenant_id as INTEGER)
    , CAST(custom_text as VARCHAR)
    -- texte informatif sur la situation du candidat locataire
    , CAST(monthly_sum as INTEGER)
    -- montant des ressources déclarées par le candidat locataire
    , CAST(guarantor_id as INTEGER)
    , CAST(document_sub_category as VARCHAR),
    CAST(document_status as VARCHAR)
    , CAST(no_document as BOOLEAN)
    -- à creuser
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(document_denied_reasons_id as INTEGER)
    , CAST(avis_detected as BOOLEAN)
    , CAST(watermark_file_id as INTEGER)
    , CAST(last_modified_date as TIMESTAMP)
from {{ source('dossierfacile', 'document') }}
{{ filter_recent_data('creation_date') }}
