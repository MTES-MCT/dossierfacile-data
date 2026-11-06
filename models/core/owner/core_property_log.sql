select * -- noqa: AM04
from {{ ref('staging_property_log') }}
