select
    checked_options
    , document_user_type
    , tenant_origin
    , case when tenant_origin ilike '%hybrid%' then 'partner' else 'organic' end as funnel_type
    , DATE(created_at) as created_at
    , COUNT(*) as nb_checked_options
from {{ ref('core_document_denied') }}
group by
    DATE(created_at)
    , checked_options
    , document_user_type
    , tenant_origin
    , case when tenant_origin ilike '%hybrid%' then 'partner' else 'organic' end 
    