select
    CAST(id as INTEGER)
    , CAST(document_category as VARCHAR)
    , CAST(document_sub_category as VARCHAR)
    , CAST(document_category_step as VARCHAR)
    , CAST(tenant_id as INTEGER)
    , CAST(monthly_sum as INTEGER) as monthly_net_income-- montant des ressources déclarées par le candidat locataire
    , CAST(guarantor_id as INTEGER)
    , CAST(document_status as VARCHAR)
    , CAST(creation_date as TIMESTAMP) as created_at
    , CAST(watermark_file_id as INTEGER)
    , CAST(last_modified_date as TIMESTAMP) as modified_at
from {{ source('dossierfacile', 'document') }}
{{ filter_recent_data('creation_date') }}
