select * -- noqa: AM04
from {{ ref('staging_document_analysis_report') }}
