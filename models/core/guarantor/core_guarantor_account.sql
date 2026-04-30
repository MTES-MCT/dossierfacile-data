-- knowns limitations: 
-- - we don't have deleted guarantor account
select * from {{ ref('staging_guarantor') }}
