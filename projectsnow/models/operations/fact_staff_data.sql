{{ 
    config(
        materialized='table',
    ) 
}}

WITH all_staff_clean AS (
    SELECT
        staff_id,
        staff_name,
        staff_username,
        staff_email,
        staff_position,
        staff_position_level,
        staff_citizenship,
        staff_residence,
        staff_offboarded_at,
        staff_created_at,
        staff_industries,
        staff_software,
        staff_styles,
        CASE 
            WHEN staff_offboarded_at IS NULL THEN 'Yes'
            ELSE 'No'
        END AS current_employment_status
    FROM {{ ref('int_all_staff') }}
),

sorted_mobility AS (
    SELECT
        staff_id,
        staff_previous_role,
        staff_previous_job_level,
        staff_previous_functional_group,
        staff_previous_manager,
        staff_date_of_mobility
    FROM {{ ref('int_staff_mobility') }}
),

aggregated_mobility AS (
    SELECT
        staff_id,
        COUNT(*) AS movement_count,
        LISTAGG(staff_previous_role, ', ') AS all_previous_roles,
        LISTAGG(staff_previous_job_level, ', ') AS all_previous_job_levels,
        LISTAGG(staff_previous_functional_group, ', ') AS all_previous_functional_groups,
        LISTAGG(staff_previous_manager, ', ') AS all_previous_managers
    FROM (
        SELECT
            staff_id,
            staff_previous_role,
            staff_previous_job_level,
            staff_previous_functional_group,
            staff_previous_manager,
            ROW_NUMBER() OVER (PARTITION BY staff_id ORDER BY staff_date_of_mobility DESC) AS row_order
        FROM sorted_mobility
    )
    GROUP BY STAFF_ID
),

current_roles AS (
    SELECT
        staff_id,
        staff_role AS latest_position
    FROM {{ ref('int_current_staff') }}
),

fact_staff_combined AS (
    SELECT
        all_staff.staff_id,
        all_staff.staff_name,
        all_staff.staff_username,
        all_staff.staff_email,
        all_staff.staff_position,
        all_staff.staff_position_level,
        all_staff.staff_citizenship,
        all_staff.staff_residence,
        all_staff.staff_industries,
        all_staff.staff_software,
        all_staff.staff_styles,
        all_staff.staff_offboarded_at,
        all_staff.staff_created_at,
        all_staff.current_employment_status,
        COALESCE(mobility.movement_count, 0) AS movement_count,
        mobility.all_previous_roles,
        mobility.all_previous_job_levels,
        mobility.all_previous_functional_groups,
        mobility.all_previous_managers,
        current_staff.latest_position
    FROM all_staff_clean AS all_staff
    LEFT JOIN aggregated_mobility AS mobility
        ON all_staff.staff_id = mobility.staff_id
    LEFT JOIN current_roles AS current_staff
        ON all_staff.staff_id = current_staff.staff_id
)

SELECT * FROM fact_staff_combined
