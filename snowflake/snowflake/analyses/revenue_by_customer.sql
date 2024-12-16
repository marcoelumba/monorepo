/*
    Top Customers by Revenue: Identify which customers contribute the most to revenue.
    Customer Engagement Trends: Analyze whether customers prefer recurring, new, or one-time engagements.
    Average Revenue per Customer: Understand revenue distribution across customers.
*/
SELECT 
    customer_id,
    customer_name,
    SUM(total_revenue_usd) AS total_revenue,
    COUNT(DISTINCT project_id) AS unique_projects,
    AVG(total_revenue_usd) AS avg_revenue_per_project
FROM customer_engagement_summary
GROUP BY customer_id, customer_name
ORDER BY total_revenue DESC;