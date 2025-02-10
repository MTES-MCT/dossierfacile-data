select
    CAST(id as INTEGER) as id
    , CAST(name as VARCHAR) as name
    -- , CAST(token as VARCHAR) as token
    , CAST(owner_id as INTEGER) as owner_id
    , CAST(creation_date as TIMESTAMP) as creation_date
    , CAST(count_visit as INTEGER) as count_visit
    , CAST(property_id as INTEGER) as property_id
    , CAST(rent_cost as BIGINT) as rent_cost
    , CAST(displayed as BOOLEAN) as displayed
    , CAST(notification as BOOLEAN) as notification
    , CAST(cant_email_sent_prospect as INTEGER) as cant_email_sent_prospect
    , CAST(validated as BOOLEAN) as validated
    , CAST(type as VARCHAR) as type
    , CAST(furniture as VARCHAR) as furniture
    , CAST(address as VARCHAR) as address
    , CAST(charges_cost as FLOAT) as charges_cost -- double
    , CAST(living_space as FLOAT) as living_space -- double
    , CAST(energy_consumption as INTEGER) as energy_consumption
    , CAST(co2emission as INTEGER) as co2emission
    , CAST(validated_date as TIMESTAMP) as validated_date
    , CAST(ademe_number as VARCHAR) as ademe_number
    , CAST(ademe_api_result as JSONB) as ademe_api_result
    , CAST(dpe_date as TIMESTAMP) as dpe_date
    , CAST(dpe_not_required as BOOLEAN) as dpe_not_required
from {{ source('dossierfacile', 'property') }}
