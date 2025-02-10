select
    CAST(id as INTEGER) as id
    , CAST(json_profile ->> 'id' as INTEGER) as tenant_id
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'id' as INTEGER) as property_id
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'name' as VARCHAR) as property_name
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'type' as VARCHAR) as property_type
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'address' as VARCHAR) as property_address
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'rentCost' as VARCHAR) as property_rent_cost
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'furniture' as VARCHAR) as property_furniture
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'validated' as BOOLEAN) as property_validated
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'chargesCost' as FLOAT) as property_charges_cost
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'co2emission' as FLOAT) as property_co2_emission
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'livingSpace' as FLOAT) as property_living_space
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'energyConsumption' as FLOAT) as property_energy_consumption
    , CAST(JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'propertiesApartmentSharing' as VARCHAR) as property_apartment_sharing
    , CAST(json_profile -> 'creationDateTime' as VARCHAR) as property_creation_date
    , CAST(json_profile -> 'franceConnect' as VARCHAR) as property_france_connect
from owner_log
where json_profile is not null
