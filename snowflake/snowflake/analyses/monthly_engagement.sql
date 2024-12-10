/*
    Engagement Trends Over Time: See how customer engagement changes month-over-month or year-over-year.
    Revenue Trends by Time: Analyze revenue patterns over time (if you have a date column).
*/
SELECT 
    DATE_TRUNC('month', engagement_date) AS month,
    SUM(total_revenue_usd) AS monthly_revenue,
    COUNT(*) AS monthly_engagements
FROM customer_engagement_summary
GROUP BY month
ORDER BY month ASC;