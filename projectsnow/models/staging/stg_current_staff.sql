{{ 
    config( 
        materialized='incremental', 
        partition_by={
            "field": "ingested_date", 
            "data_type": "date",
            "granularity": "day"}
    ) 
}}

WITH raw_current_staff AS (
    SELECT
        {{ clean_na_null('name') }} AS name,
        REPLACE(REPLACE(LOWER( nullif( {{ clean_na_null('role') }} ,'#REF!')),',',''),'-','') AS role,
        {{ clean_email_format('email') }} AS email, 
        COALESCE(nullif(gender, '#N/A'), 'Not mentioned') AS gender, 
        {{ clean_date_format('birthday') }} AS birthday,
        {{ clean_job_level_format('job_level') }} AS job_level,
        {{ clean_country_format('residence') }} AS residence,
        {{ clean_date_format('start_date') }} AS start_date,
        {{ clean_country_format('nationality') }} AS nationality,
        {{ clean_na_null('manager_email') }} AS manager_email,
        {{ clean_na_null('business_group') }} AS business_group,
        {{ clean_email_format('personal_email') }} AS personal_email,
        CURRENT_TIMESTAMP() AS ingested_date,
        'superside_raw.hr_current_staff' AS ingested_from
    FROM {{ source('superside_raw','hr_current_staff') }} 
)

SELECT *
FROM raw_current_staff
