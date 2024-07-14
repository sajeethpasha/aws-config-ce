WITH last_seven_days AS (
    SELECT
        date_parse(date, '%Y-%m-%d') AS date,
        SUM(CAST(service_cost AS double)) AS "Service Cost"
    FROM
        "aws-costs"."cost"
    WHERE
        date_parse(date, '%Y-%m-%d') >= current_date - interval '7' day
    GROUP BY
        date_parse(date, '%Y-%m-%d')
),
first_and_last AS (
    SELECT
        MIN(date) AS first_date,
        MAX(date) AS last_date
    FROM
        last_seven_days
)
SELECT
    date,
    "Service Cost"
FROM
    last_seven_days
WHERE
    date IN (
        SELECT first_date FROM first_and_last
        UNION
        SELECT last_date FROM first_and_last
    );
