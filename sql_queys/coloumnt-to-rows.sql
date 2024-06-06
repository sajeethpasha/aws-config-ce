SELECT 
    metric,
    MAX(CASE WHEN "Source" = 'green-by-product.csv' THEN value END) AS green,
    MAX(CASE WHEN "Source" = 'red-by-product.csv' THEN value END) AS red
FROM (
	
    (SELECT 
        'performsfapi' AS metric, 
        "Source", 
        performsfapi AS value
    FROM products order by "Date"  desc limit 2)
    UNION ALL
    (SELECT 
        'capabilities' AS metric, 
        "Source", 
        capabilities AS value
    FROM products order by "Date"  desc limit 2)
    UNION ALL
    (SELECT 
        'intelligenttrialsapi' AS metric, 
        "Source", 
        intelligenttrialsapi AS value
    FROM products order by "Date"  desc Limit 2 )
) AS subquery
GROUP BY metric;