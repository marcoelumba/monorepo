{{ 
    config( 
        materialized='table',
    ) 
}}

with stg_all_staff as (
    SELECT
        staff_id AS staff_id,
        name AS staff_name,
        username AS staff_username,
        email AS staff_email,
        position AS staff_position,
        position_level AS staff_position_level,
        styles AS staff_styles,
        industries AS staff_industries,
        software AS staff_software,
        citizenship AS staff_citizenship,
        residence AS staff_residence,
        offboarded_at AS staff_offboarded_at,
        created_at AS staff_created_at,
    FROM {{ ref('stg_all_staff') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY staff_id ORDER BY ingested_date DESC) = 1
)

SELECT *
FROM stg_all_staff
