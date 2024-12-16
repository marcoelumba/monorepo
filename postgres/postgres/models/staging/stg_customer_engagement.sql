
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with raw_engagement as (
    SELECT
         "Project ID" AS project_id,
         "EngagementID" AS engagement_id,
         "Customer ID" AS customer_id,
         "Customer Name" AS customer_name,
         REPLACE(REPLACE("Engagement Date", '.', '/'), '-', '/') AS engagement_date,
         "Revenue" AS revenue,
         "Revenue USD" AS revenue_usd,
         "Service" AS service,
         "Sub-Service" AS sub_service,
         "Engagement Type" AS engagement_type,
         "Employee Count" AS employee_count,
         "Comments" AS comments,
         "Project Ref" AS project_reference,
         "Engagement Reference" AS engagement_reference,
         "Client Revenue" AS client_revenue,
         "Service Type" AS service_type,
         "Detailed Sub-Service" AS detailed_sub_service
    from raw.customer_engagement
), 

clean_raw_data AS (
    SELECT
         project_id,
         engagement_id,
         customer_id,
         {{ clean_customer_name('customer_name') }} AS customer_name,
         {{ normalize_date('engagement_date')  }} AS engagement_date,
         {{ clean_numeric_value('revenue') }} AS revenue,
         {{ clean_numeric_value('revenue_usd') }} AS revenue_usd,
         {{ replace_with_mapping('service') }} AS "service",
         {{ replace_with_mapping('sub_service') }} AS sub_service,
         {{ replace_with_mapping('engagement_type') }} AS engagement_type,
         {{ convert_word_to_number('employee_count') }} AS employee_count,
         comments,
         project_reference,
         engagement_reference,
         {{ clean_numeric_value('client_revenue') }} AS client_revenue,
         {{ replace_with_mapping('service_type') }} AS service_type,
         {{ replace_with_mapping('detailed_sub_service') }} AS detailed_sub_service
    FROM raw_engagement
)


SELECT * FROM clean_raw_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
