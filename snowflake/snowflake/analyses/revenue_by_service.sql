/*
    Revenue by Service Type: See which service types contribute most to your business.
    Engagement Count by Service Type: Understand which services are most commonly engaged by customers.
    Engagement Type Distribution by Service: Break down engagement types (New, Recurring, One-time) by service type.
*/
SELECT 
    service_type,
    SUM(total_revenue_usd) AS total_revenue,
    COUNT(*) AS total_engagements,
    SUM(new_engagements) AS new_engagements,
    SUM(recurring_engagements) AS recurring_engagements,
    SUM(one_time_engagements) AS one_time_engagements
FROM customer_engagement_summary
GROUP BY service_type
ORDER BY total_revenue DESC;