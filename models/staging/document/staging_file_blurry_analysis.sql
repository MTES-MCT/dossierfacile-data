with blurry_analysis_parsed as (
    select
        CAST(id as INTEGER) as id
        , CAST(data_file_id as INTEGER) as file_id
        , CAST(data_document_id as INTEGER) as document_id
        , CAST(analysis_status as VARCHAR) as analysis_status
        , CAST(blurry_results as JSONB) as blurry_results_json
    from {{ source('dossierfacile', 'blurry_file_analysis') }}
    where id > 8052310 -- new blurry detection logic based on OCR has been enabled on 2025-10-30 00:00:00
)

, blurry_results_flattened as (
    select
        id
        , file_id
        , document_id
        , analysis_status
        , blurry_results_json ->> 'isBlank' as is_blank
        , blurry_results_json ->> 'isBlurry' as is_blurry
        , CAST(blurry_results_json ->> 'ocrTokens' as INTEGER) as ocr_token_count
        , CAST(blurry_results_json ->> 'ocrMeanScore' as DECIMAL(5, 2)) as ocr_mean_score
    from blurry_analysis_parsed
)

select
    id
    , file_id
    , document_id
    , analysis_status
    , ocr_token_count
    , ocr_mean_score
    , CAST(is_blank as BOOLEAN) as is_blank
    , CAST(is_blurry as BOOLEAN) as is_blurry
from blurry_results_flattened
