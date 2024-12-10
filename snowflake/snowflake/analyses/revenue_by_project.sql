/*
    High-Performing Projects: Identify which projects generate the highest revenue or engagement counts.
    Project Status Analysis: See how project statuses (e.g., Active, Completed, Cancelled) impact engagement and revenue.
    Engagement Diversity by Project: Measure how many engagement types (New, Recurring, One-time) are present for each project.
*/

SELECT 
    project_id,
    project_name,
    project_status,
    SUM(total_revenue_usd) AS project_revenue,
    COUNT(*) AS total_engagements,
    SUM(new_engagements) AS new_engagements,
    SUM(recurring_engagements) AS recurring_engagements,
    SUM(one_time_engagements) AS one_time_engagements
FROM customer_engagement_summary
GROUP BY project_id, project_name, project_status
ORDER BY project_revenue DESC;