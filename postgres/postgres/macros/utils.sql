{% macro clean_customer_name(customer_name) %}
    CASE
        -- If the value is NULL, assign a default value
        WHEN {{ customer_name }} IS NULL THEN NULL
        
        -- Extract the number using SUBSTRING, default to 0 if no match is found
        ELSE CONCAT(
            'Customer_',
            COALESCE(
                SUBSTRING({{ customer_name }} FROM '\d+'), 
                '0'
            )
        )
    END
{% endmacro %}


{% macro normalize_date(column_name) %}
    CASE
        -- Handle YYYY/MM/DD
        WHEN {{ column_name }} ~ '^\d{4}/\d{2}/\d{2}$' AND SUBSTRING({{ column_name }} FROM 6 FOR 2)::INTEGER > 12
            THEN TO_DATE({{ column_name }}, 'YYYY/DD/MM')

         -- Handle YYYY/MM/DD
        WHEN {{ column_name }} ~ '^\d{4}/\d{2}/\d{2}$' AND SUBSTRING({{ column_name }} FROM 9 FOR 2)::INTEGER > 12
            THEN TO_DATE({{ column_name }}, 'YYYY/MM/DD')

        -- Handle MM/DD/YYYY (detect if the first part cannot be day > 12)
        WHEN {{ column_name }} ~ '^\d{2}/\d{2}/\d{4}$' AND SUBSTRING({{ column_name }} FROM 1 FOR 2)::INTEGER > 12
            THEN TO_DATE({{ column_name }}, 'DD/MM/YYYY')

        -- Handle DD/MM/YYYY (assume valid if day <= 12 has not triggered)
        WHEN {{ column_name }} ~ '^\d{2}/\d{2}/\d{4}$' AND SUBSTRING({{ column_name }} FROM 4 FOR 2)::INTEGER > 12
            THEN TO_DATE({{ column_name }}, 'MM/DD/YYYY')

        -- Default: If none of the above matches, return NULL
        ELSE NULL
    END
{% endmacro %}

{% macro clean_numeric_value(column_name) %}
    -- Step-by-step cleaning process
    CASE
        -- Step 1: Handle values ending with 'k' (e.g., "10k" -> "10000")
        WHEN {{ column_name }} ~ '[0-9]+k$' THEN 
            CAST(REGEXP_REPLACE(revenue,'[[:alpha:]$]','','g') AS NUMERIC) * 1000

        -- Step 2: Remove currency signs, letters, and retain only numbers and decimals
        ELSE CAST(REGEXP_REPLACE(revenue,'[[:alpha:]$]','','g') AS NUMERIC)
    END
{% endmacro %}

{% macro replace_with_mapping(column_name) %}
    CASE
        -- Replace words based on the mapping table
        WHEN EXISTS (
            SELECT 1 
            FROM common.word_mapping 
            WHERE word_mapping.word = {{ column_name }}
        ) THEN (
            SELECT correct_word 
            FROM common.word_mapping 
            WHERE word_mapping.word = {{ column_name }}
        )
        -- If no match is found, keep the original value
        ELSE {{ column_name }}
    END
{% endmacro %}

{% macro convert_word_to_number(column_name) %}
    COALESCE(
        (
            SELECT number
            FROM staging.number_mapping
            WHERE LOWER({{ column_name }}) = word
        ),
        CAST( {{ column_name }} AS INTEGER) -- Fallback to original value if no match
    )
{% endmacro %}