{% macro clean_email_format(email_field) %}
    CASE
        WHEN regexp_like({{ email_field }}, '^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$') THEN {{ email_field }}
        ELSE NULL
    END
{% endmacro %}

{% macro clean_date_format(date_field) %}
    CASE
        WHEN TRY_TO_DATE(COALESCE(nullif( {{ date_field }}, '#N/A'), NULL), 'YYYY-MM-DD') IS NOT NULL THEN {{ date_field }}
        ELSE NULL
    END
{% endmacro %}

{% macro clean_job_level_format(job_level_field) %}
    CASE 
        WHEN regexp_like(coalesce(nullif( {{ job_level_field }}, '#N/A'),NULL),'^(\\d+[a-c](\\s(IC|Manager))?|[1-8])$') THEN {{ job_level_field }}
        ELSE NULL
    END 
{% endmacro %}

{% macro clean_country_format(country_field) %}
    CASE 
        WHEN COALESCE(nullif( {{ country_field }}, '#N/A'), NULL) = 'United States of America (USA)' THEN 'United States of America'
        ELSE TRIM(COALESCE(nullif( {{ country_field }}, '#N/A'), NULL))
    END
{% endmacro %}

{% macro clean_na_null(null_field) %}
    COALESCE(nullif(nullif( {{ null_field }}, '#N/A'), 'N/A'), NULL)
{% endmacro %}

REPLACE(REPLACE(LOWER( COALESCE(nullif(nullif(TRIM(previous_role),'#N/A'),'#REF!'), NULL) ),',',''),'-','')

{% macro clean_string_to_aray(array_field) %}
    SPLIT( REGEXP_SUBSTR( TRIM( COALESCE(nullif( {{ array_field }} , '[]'), NULL) ), '\\[(.*)\\]', 1, 1, 'e'), ',' )
{% endmacro %}


