-- knowns limitations: 
-- - we don't have deleted guarantor account
select * -- noqa: AM04
from {{ ref('staging_guarantor') }}
