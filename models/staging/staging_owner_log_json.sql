with json_details as (
    select
        id
        , json_profile ->> 'id' as owner_id
        , json_profile ->> 'creationDateTime' as owner_created_at        -- date de creation du compte du compte proprietaire
        , json_profile ->> 'franceConnect' as owner_france_connect
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'id' as property_id
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'name' as property_name
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'type' as property_type
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'address' as property_address
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'rentCost' as property_rent_cost
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'furniture' as property_furniture
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'validated' as property_validated
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'chargesCost' as property_charges_cost
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'co2emission' as property_co2_emission
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'livingSpace' as property_living_space
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'energyConsumption' as property_energy_consumption
        , JSONB_ARRAY_ELEMENTS(json_profile -> 'properties') ->> 'propertiesApartmentSharing' as property_apartment_sharing
    from {{ source('dossierfacile', 'owner_log') }}
    where json_profile is not null
)

, casting_logs as (
    select
        CAST(owner_log.id as INTEGER) as id
        , CAST(owner_log.owner_id as INTEGER) as owner_id
        , CAST(owner_log.creation_date as VARCHAR) as owner_deleted_at
        , CAST(owner_log.log_type as VARCHAR) as log_type
        , CAST(json_details.owner_created_at as VARCHAR) as owner_created_at
        , CAST(json_details.owner_france_connect as BOOLEAN) as owner_france_connect
        , CAST(json_details.property_id as INTEGER) as property_id
        , CAST(json_details.property_name as VARCHAR) as property_name
        , CAST(json_details.property_type as VARCHAR) as property_type
        , CAST(json_details.property_address as VARCHAR) as property_address
        , CAST(json_details.property_rent_cost as VARCHAR) as property_rent_cost
        , CAST(json_details.property_furniture as VARCHAR) as property_furniture
        , CAST(json_details.property_validated as BOOLEAN) as property_validated
        , CAST(json_details.property_charges_cost as FLOAT) as property_charges_cost
        , CAST(json_details.property_co2_emission as FLOAT) as property_co2_emission
        , CAST(json_details.property_living_space as FLOAT) as property_living_space
        , CAST(json_details.property_energy_consumption as FLOAT) as property_energy_consumption
        , CAST(json_details.property_apartment_sharing as VARCHAR) as property_apartment_sharing
    from {{ source('dossierfacile', 'owner_log') }} as owner_log
    inner join json_details on owner_log.id = json_details.id
)

select
    id
    , owner_id
    , owner_deleted_at
    , log_type
    , owner_created_at
    , owner_france_connect
    , property_id
    , property_name
    , property_type
    , property_address
    , property_rent_cost
    , property_furniture
    , property_validated
    , property_charges_cost
    , property_co2_emission
    , property_living_space
    , property_energy_consumption
    , property_apartment_sharing
from casting_logs
