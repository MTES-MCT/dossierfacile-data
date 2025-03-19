with completion_delay as (
    select
        id
        , DATE_TRUNC('week', created_at) as created_at
        , case
            when 1.0 * EXTRACT(epoch from first_completion_at - created_at) / 3600 < 1 then 1.0 * EXTRACT(epoch from first_completion_at - created_at) / 60
            else 1.0 * EXTRACT(epoch from first_completion_at - created_at) / 3600
        end as time_to_complete
    from {{ ref('core_tenant_account') }}
    where completion_flag = 1
), completion_percentile as (

    select
        created_at
        , PERCENTILE_CONT(0.25) within group (
            order by time_to_complete
        ) as q25
        , PERCENTILE_CONT(0.5) within group (
            order by time_to_complete
        ) as q50
        , PERCENTILE_CONT(0.75) within group (
            order by time_to_complete
        ) as q75
    from completion_delay
    group by 1

)

select 
created_at
, ROUND(q25::decimal,2) as q25
, ROUND(q50::decimal,2) as q50
, ROUND(q75::decimal,2) as q75
from completion_percentile