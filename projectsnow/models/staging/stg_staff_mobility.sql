{{ 
    config( 
        materialized='incremental',
        unique_key='unique_key', 
        partition_by={
            "field": "DATE_TRUNC(ingested_date, YEAR)", 
            "data_type": "date"}
    ) 
}}

with raw_staff_mobility as (
    SELECT
        md5(CONCAT(name, date_of_mobility)) AS unique_key,
        {{ clean_na_null('name') }} AS name,
        REPLACE(REPLACE(LOWER( nullif( {{ clean_na_null('previous_role') }} ,'#REF!')),',',''),'-','') AS previous_role,
        {{ clean_date_format('date_of_mobility') }} AS date_of_mobility,
        {{ clean_na_null('previous_manager') }} AS previous_manager,
        {{ clean_job_level_format('previous_job_level') }} AS previous_job_level,
        {{ clean_na_null('previous_functional_group') }} AS previous_functional_group,
        CURRENT_TIMESTAMP() AS ingested_date,
        'superside_raw.hr_staff_mobility' AS ingested_from
    FROM {{ source('superside_raw','hr_staff_mobility') }}
    WHERE name IS NOT NULL AND name != '#N/A'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY name, date_of_mobility ORDER BY date_of_mobility DESC) = 1
)

SELECT *
FROM raw_staff_mobility
{% if is_incremental() %}
    WHERE unique_key NOT IN (
        SELECT unique_key
        FROM {{ this }}
    )
{% endif %}
