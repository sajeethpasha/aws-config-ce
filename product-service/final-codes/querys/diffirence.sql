WITH monthly_totals AS (
    SELECT
        DATE_TRUNC('month', Service) AS month,
        SUM(TotalCost) AS total_cost
    FROM
        services
    WHERE
        Source = 'red-by-service.csv'
    GROUP BY
        DATE_TRUNC('month', Service)
)
SELECT
    current_month.month AS month,
    current_month.total_cost AS current_month_total,
    previous_month.total_cost AS previous_month_total,
    CASE
        WHEN previous_month.total_cost IS NULL THEN NULL
        WHEN previous_month.total_cost = 0 THEN NULL
        ELSE ((current_month.total_cost - previous_month.total_cost) / previous_month.total_cost) * 100
    END AS percentage_increment
FROM
    monthly_totals current_month
LEFT JOIN
    monthly_totals previous_month
ON
    current_month.month = previous_month.month + INTERVAL '1 month'
ORDER BY
    current_month.month;
