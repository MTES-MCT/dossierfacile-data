select 
cast(id as integer)
-- , cast(access_full as boolean)
-- access_full est toujours à `true`
, cast(apartment_sharing_id as integer)
, cast(property_id as integer)
, cast(token as varchar)
from property_apartment_sharing
