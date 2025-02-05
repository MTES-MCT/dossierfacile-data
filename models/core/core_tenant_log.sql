select *
from {{ ref('staging_tenant_log') }}
