SELECT
    date_parse(date, '%Y-%m-%d') AS date,
    SUM(CAST(service_cost AS double)) AS "Service Cost"
FROM
    "aws-costs"."cost"
WHERE
    date_parse(date, '%Y-%m-%d') = DATE '2024-06-26'
GROUP BY
    date_parse(date, '%Y-%m-%d');