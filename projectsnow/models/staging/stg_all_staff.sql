{{ 
    config( 
        materialized='incremental',
        unique_key='unique_key', 
        partition_by={
            "field": "DATE_TRUNC(ingested_date, YEAR)", 
            "data_type": "date"}
    ) 
}}

with raw_all_staff as (
    SELECT
        HASH(CONCAT( COALESCE(staff_id,0), 
                    COALESCE(name,''), 
                    COALESCE(username,''), 
                    COALESCE(email,''), 
                    COALESCE(position,''), 
                    COALESCE(position_level,0),
                    COALESCE(styles,''), 
                    COALESCE(industries,''), 
                    COALESCE(software,''), 
                    COALESCE(citizenship,''), 
                    COALESCE(residence,''),
                    COALESCE(TO_CHAR(offboarded_at, 'YYYY-MM-DD HH24:MI:SS.FF3'), '') 
                    )
                ) AS unique_key,
        staff_id,
        {{ clean_na_null('name') }} AS name,
        {{ clean_na_null('username') }} AS username,
        {{ clean_email_format('email') }} AS email,
        REPLACE(LOWER({{ clean_na_null('position') }}),'_',' ') AS position,
        position_level,
        {{ clean_string_to_aray('styles') }} AS styles,
        {{ clean_string_to_aray('industries') }} AS industries,
        {{ clean_string_to_aray('software') }}  AS software,
        {{ clean_country_format('citizenship') }} AS citizenship,
        {{ clean_country_format('residence') }} AS residence,
        DATE(offboarded_at) AS offboarded_at,
        TO_DATE(created_at, 'DD/MM/YYYY HH24:MI') AS created_at,
        CURRENT_TIMESTAMP() AS ingested_date,
        'superside_raw.all_staff' AS ingested_from
    FROM {{ source('superside_raw','all_staff') }}
)

SELECT *
FROM raw_all_staff
{% if is_incremental() %}
    WHERE unique_key NOT IN (
        SELECT unique_key
        FROM {{ this }}
    )
{% endif %}
