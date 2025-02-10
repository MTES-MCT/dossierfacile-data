with fill_na as (
    select 
    tenant_id, 
    userapi_id, 
    case when access_granted_date is null then '1970-01-01' else access_granted_date end as access_granted_date
    from tenant_userapi
), first_access_granted as (
    select 
    tenant_id, 
    min(access_granted_date) as first_access_granted_date
    from fill_na
    group by tenant_id
)
select
cast(fill_na.tenant_id as INTEGER) as tenant_id
, cast(min(case when first_access_granted_date = '1970-01-01' then null else first_access_granted_date end) as TIMESTAMP) as access_granted_date
, cast(min(fill_na.userapi_id) as INTEGER) as partner_client_id
from fill_na
inner join first_access_granted on fill_na.tenant_id = first_access_granted.tenant_id and fill_na.access_granted_date = first_access_granted.first_access_granted_date
group by 1 


/*
select
    CAST(tenant_id as INTEGER) as tenant_id
    , CAST(userapi_id as INTEGER) as partner_client_id
    , CAST(access_granted_date as TIMESTAMP) as access_granted_date
from {{ source('dossierfacile', 'tenant_userapi') }}
{{ filter_recent_data('access_granted_date') }}
*/