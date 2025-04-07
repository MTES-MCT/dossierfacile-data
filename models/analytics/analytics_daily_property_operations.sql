select
    DATE(created_at) as creation_date
    , SUM(case when log_type = 'APPLICATION_DELETED_BY_OWNER' then 1 else 0 end) as application_deleted_by_owner
    , SUM(case when log_type = 'APPLICATION_PAGE_VISITED' then 1 else 0 end) as application_page_visited
    , SUM(case when log_type = 'APPLICATION_RECEIVED' then 1 else 0 end) as application_received
from {{ ref('core_property_log') }}
group by DATE(created_at)
