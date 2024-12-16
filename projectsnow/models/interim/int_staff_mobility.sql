{{ 
    config( 
        materialized='table',
    ) 
}}
 
WITH stg_all_staff AS (
    SELECT 
        DISTINCT staff_id,
        name
    FROM {{ ref('stg_all_staff') }}
),

stg_staff_mobility as (
    SELECT
        sas.staff_id AS staff_id,
        ssm.name AS staff_name,
        ssm.previous_role AS staff_previous_role,
        ssm.date_of_mobility AS staff_date_of_mobility,
        ssm.previous_manager AS staff_previous_manager,
        ssm.previous_job_level AS staff_previous_job_level,
        ssm.previous_functional_group AS staff_previous_functional_group
    FROM {{ ref('stg_staff_mobility') }} AS ssm 
    LEFT OUTER JOIN stg_all_staff AS sas
        ON ssm.name = sas.name
    QUALIFY ROW_NUMBER() OVER (PARTITION BY ssm.unique_key ORDER BY ssm.ingested_date DESC) = 1
)

SELECT *
FROM stg_staff_mobility
