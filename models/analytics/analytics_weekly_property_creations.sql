with property_creations as (
    select
        owner_id
        , created_at
        , ROW_NUMBER() over (
            partition by owner_id
            order by created_at asc
        ) as row_number
    from {{ ref('staging_property') }}
)

select
    DATE_TRUNC('week', created_at) as created_at
    , SUM(case when row_number = 1 then 1 else 0 end) as first_properties
    , SUM(case when row_number > 1 then 1 else 0 end) as multiple_properties
from property_creations
where row_number = 1
group by 1
