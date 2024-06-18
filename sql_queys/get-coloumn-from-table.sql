SELECT column_name
FROM information_schema.columns
WHERE table_name = 'products'
and column_name  not in ('Date','id')