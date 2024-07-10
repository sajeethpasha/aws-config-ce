SELECT gr.product, gr."Green", rd."Red"
FROM
  (SELECT product, sum(cost) AS "Green"
   FROM aws_monthly_costs
   WHERE account_name = 'aws-green R&D'
     AND date_parse("date", '%Y-%m-%d') = date '2024-08-01'
   GROUP BY product
   ORDER BY product) gr
FULL JOIN
  (SELECT product, sum(cost) AS "Red"
   FROM aws_monthly_costs
   WHERE account_name = 'aws-red'
     AND date_parse("date", '%Y-%m-%d') = date '2024-08-01'
   GROUP BY product
   ORDER BY product) rd
ON gr.product = rd.product
ORDER BY gr.product;
