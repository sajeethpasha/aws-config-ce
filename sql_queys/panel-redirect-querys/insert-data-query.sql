

	INSERT INTO public.products_new (id, "Date", performsfapi, capabilities, "Source")
VALUES 
(10, '2024-06-18', 99.99, 88.88, 'example_source');

-- ----------------------------------------------------------

INSERT INTO public.services_new (
    id,
    "Date",
    "ElastiCache",
    "Lambda",
    capabilities,
    loadbalancing,
    "Ec2Instances",
    "Ec2Other",
    "S3",
    "SQS",
    "Inspector",
    "AppSync",
    "Shield",
    "ApiGateWay",
    "ElasticContainerService",
    "TotalCost",
    "Source",
    "products_id"
)
VALUES (
    -- Replace with actual values
    13, -- id (bigint)
    '2024-06-18', -- Date (text)
    10.50, -- ElastiCache (double precision)
    2.25, -- Lambda (double precision)
    7.00, -- capabilities (double precision)
    1.75, -- loadbalancing (double precision)
    50.00, -- Ec2Instances (double precision)
    12.10, -- Ec2Other (double precision)
    3.75, -- S3 (double precision)
    0.50, -- SQS (double precision)
    1.25, -- Inspector (double precision)
    0.00, -- AppSync (double precision)
    0.00, -- Shield (double precision)
    2.10, -- ApiGateWay (double precision)
    1, -- ElasticContainerService (bigint)
    77.20, -- TotalCost (double precision)
    'AWS Cost Report', -- Source (text)
    'capabilities' -- products_id (text)
);



