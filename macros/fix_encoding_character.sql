{% macro fix_encoding_character(field) %}
    REGEXP_REPLACE(
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                {{ field }}
                                                , '<\/*[a-zA-Z0-9]*[^>]*>', '', 'g')
                                            , 'u00b7', '·', 'g')
                                        , 'u00e7', 'ç', 'g')
                                    , '&quot;', '"', 'g')
                                , 'u00f4', 'ô', 'g')
                            , 'u00e8', 'è', 'g')
                        , 'u2019', '''', 'g')
                    , 'u00e9', 'é', 'g')
                , 'u00e0', 'à', 'g')
            , 'u00ea', 'ê', 'g')
        , 'u00a0', 'é', 'g')
{% endmacro %}