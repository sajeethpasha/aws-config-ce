CREATE TABLE Sales (
    Product VARCHAR(50),
    Year INT,
    Sales INT
);

INSERT INTO Sales (Product, Year, Sales) VALUES
('A', 2021, 100),
('A', 2022, 150),
('B', 2021, 200),
('B', 2022, 250);


CREATE EXTENSION IF NOT EXISTS tablefunc;


SELECT *
FROM crosstab(
  $$SELECT Product, Year, Sales FROM Sales ORDER BY Product, Year$$
) AS ct (Product VARCHAR, Sales_2021 INT, Sales_2024 INT);



