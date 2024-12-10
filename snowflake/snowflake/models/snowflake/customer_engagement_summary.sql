
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

WITH joined_data AS (
    SELECT 
        sce.customer_id,
        sce.customer_name,
        sce.project_id,
        sce.engagement_type,
        sce.service_type,
        sce.revenue_usd,
        dp.NAME AS project_name,
        dp.STATUS AS project_status
    FROM {{ source('snowflake', 'STG_CUSTOMER_ENGAGEMENT') }} AS sce
    JOIN {{ source('snowflake', 'DIM_PROJECT') }} AS dp
    ON sce.project_id = dp.PROJECT_ID
),

engagement_summary AS (
    -- Aggregate customer engagement insights
    SELECT
        customer_id,
        customer_name,
        project_id,
        project_name,
        project_status,
        service_type,
        engagement_type,
        SUM(revenue_usd) AS total_revenue_usd,
        COUNT(*) AS engagement_count,
        SUM(CASE WHEN engagement_type = 'New' THEN 1 ELSE 0 END) AS new_engagements,
        SUM(CASE WHEN engagement_type = 'Recurring' THEN 1 ELSE 0 END) AS recurring_engagements,
        SUM(CASE WHEN engagement_type = 'One-time' THEN 1 ELSE 0 END) AS one_time_engagements
    FROM joined_data
    GROUP BY 
        customer_id,
        customer_name,
        project_id,
        project_name,
        project_status,
        service_type,
        engagement_type
)

SELECT 
    customer_id,
    customer_name,
    project_id,
    project_name,
    project_status,
    service_type,
    engagement_type,
    total_revenue_usd,
    engagement_count,
    new_engagements,
    recurring_engagements,
    one_time_engagements
FROM engagement_summary

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
