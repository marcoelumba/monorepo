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
stg_current_staff AS (
    SELECT
        sas.staff_id AS staff_id,
        scs.name AS staff_name,
        scs.role AS staff_role,
        COALESCE(scs.email, scs.personal_email) AS staff_email, 
        scs.gender AS staff_gender, 
        scs.birthday AS staff_birthday,
        scs.job_level AS staff_job_level,
        scs.residence AS staff_residence,
        scs.start_date AS staff_start_date,
        scs.nationality AS staff_citizenship,
        scs.manager_email AS staff_manager_email,
        scs.business_group AS staff_business_group,
        scs.personal_email AS staff_personal_email
    FROM {{ ref('stg_current_staff') }} AS scs
    LEFT OUTER JOIN stg_all_staff AS sas
        ON scs.name = sas.name
    WHERE DATE(scs.ingested_date) = DATE(CURRENT_TIMESTAMP())
    QUALIFY ROW_NUMBER() OVER (PARTITION BY scs.name,  COALESCE(scs.email, scs.personal_email) ORDER BY scs.ingested_date DESC) = 1 
)

SELECT *
FROM stg_current_staff
