select
    CAST(id as INTEGER)
    , CAST(creation_date as TIMESTAMP) as shared_at
    , CAST(full_data as BOOLEAN) as shared_with_documents
    , CAST(disabled as BOOLEAN) as is_link_disabled
    -- , cast(email as varchar) as shared_to_email 
    -- données personnelles 
    , CAST(apartment_sharing_id as INTEGER)
    , CAST(token as VARCHAR)
    , CAST(link_type as VARCHAR)
    , CAST(last_sent_datetime as TIMESTAMP) as last_sent_at
from apartment_sharing_link
{{ filter_recent_data('last_sent_datetime') }}
