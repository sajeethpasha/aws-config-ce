

SELECT to_char("Date", 'YYYY-MON') AS formatted_date
FROM test_products;	


SELECT *
FROM test_products
WHERE to_char("Date", 'YYYY-MON') = '2023-APR';

#grafana dynamic

select ${coloumns:csv} from test_products 


