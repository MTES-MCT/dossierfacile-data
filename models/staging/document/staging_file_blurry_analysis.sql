with blurry_analysis_parsed as (
    select
        CAST(id as INTEGER) as id
        , CAST(data_file_id as INTEGER) as file_id
        , CAST(data_document_id as INTEGER) as document_id
        , CAST(analysis_status as VARCHAR) as analysis_status
        , CAST(blurry_results as JSONB) as blurry_results_json
    from {{ source('dossierfacile', 'blurry_file_analysis') }}
    where id > 6273288 -- blurry detection has been enabled on 2025-08-29 00:00:00
)

, blurry_results_flattened as (
    select
        id
        , file_id
        , document_id
        , analysis_status
        , blurry_results_json ->> 'isBlank' as is_blank
        , blurry_results_json ->> 'isBlurry' as is_blurry
        , blurry_results_json ->> 'isReadable' as is_readable
        , CAST(blurry_results_json ->> 'laplacianVariance' as DECIMAL(15, 2)) as laplacian_variance
    from blurry_analysis_parsed
)

select
    id
    , file_id
    , document_id
    , analysis_status
    , laplacian_variance
    , CAST(is_blank as BOOLEAN) as is_blank
    , CAST(is_blurry as BOOLEAN) as is_blurry
    , CAST(is_readable as BOOLEAN) as is_readable
from blurry_results_flattened
