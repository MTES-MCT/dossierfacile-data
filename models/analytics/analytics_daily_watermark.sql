-- FiligraneFacile

select
    DATE(created_at) as creation_date
    , COUNT(id) as nb_watermark
from {{ ref('core_watermark') }}
group by DATE(created_at)
order by DATE(created_at) desc
