
CREATE TABLE IF NOT EXISTS public.products_new
(
    id bigint,
    "Date" text COLLATE pg_catalog."default",
    performsfapi double precision,
    capabilities double precision,
    "Source" text COLLATE pg_catalog."default"
)

----- -------------------------


CREATE TABLE IF NOT EXISTS public.services_new
(
    id bigint,
    "Date" text COLLATE pg_catalog."default",
    "ElastiCache" double precision,
    "Lambda" double precision,
    capabilities double precision,
    loadbalancing double precision,
    "Ec2Instances" double precision,
    "Ec2Other" double precision,
    "S3" double precision,
    "SQS" double precision,
    "Inspector" double precision,
    "AppSync" double precision,
    "Shield" double precision,
    "ApiGateWay" double precision,
    "ElasticContainerService" bigint,
    "TotalCost" double precision,
    "Source" text COLLATE pg_catalog."default",
	"products_id" text
)