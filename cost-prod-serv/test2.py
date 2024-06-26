import pandas as pd
# from sqlalchemy import create_engine
from io import StringIO

# Database connection details
db_username = 'your_username'
db_password = 'your_password'
db_host = 'your_host'
db_port = 'your_port'
db_name = 'your_dbname'

# Create the database engine
# engine = create_engine(f'postgresql+psycopg2://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}')

# Data
data = """product,account,account_id,service,cost,total_product_cost,start_date,end_date
edgesfui,aws-green R&D,767904627276,AWS Shield,0.0156901578,116.5820155407,2024-06-01,2024-06-30
edgesfui,aws-green R&D,767904627276,Amazon Elastic Container Service,0,116.5820155407,2024-06-01,2024-06-30
edgesfui,aws-green R&D,767904627276,Amazon Elastic Load Balancing,116.5663253829,116.5820155407,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,AWS AppSync,2.06922,4618.5047295657,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,AWS Lambda,4613.173888262,4618.5047295657,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,Amazon API Gateway,0.0000739883,4618.5047295657,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-green R&D,767904627276,Amazon Simple Queue Service,3.2615473154,4618.5047295657,2024-06-01,2024-06-30
intelligent-trials-model-services,aws-green R&D,767904627276,Amazon SageMaker,704.0260000764,704.0260000764,2024-06-01,2024-06-30
intelligent-trials-model-services-extension,aws-green R&D,767904627276,AWS Lambda,242.079158684,242.079158684,2024-06-01,2024-06-30
intelligenttrialsapi,aws-green R&D,767904627276,Amazon ElastiCache,93.636,277.2251315066,2024-06-01,2024-06-30
intelligenttrialsapi,aws-green R&D,767904627276,EC2 - Other,6.2720638187,277.2251315066,2024-06-01,2024-06-30
intelligenttrialsapi,aws-green R&D,767904627276,Amazon Elastic Compute Cloud - Compute,137.52,277.2251315066,2024-06-01,2024-06-30
intelligenttrialsapi,aws-green R&D,767904627276,Amazon Elastic Container Service,0,277.2251315066,2024-06-01,2024-06-30
intelligenttrialsapi,aws-green R&D,767904627276,Amazon Elastic Load Balancing,38.8000476879,277.2251315066,2024-06-01,2024-06-30
intelligenttrialsapi,aws-green R&D,767904627276,Amazon Inspector,0.99702,277.2251315066,2024-06-01,2024-06-30
intelligenttrialscapabilities,aws-green R&D,767904627276,Amazon ElastiCache,1160.169,1238.6870851733,2024-06-01,2024-06-30
intelligenttrialscapabilities,aws-green R&D,767904627276,Amazon Elastic Container Service,0,1238.6870851733,2024-06-01,2024-06-30
intelligenttrialscapabilities,aws-green R&D,767904627276,Amazon Relational Database Service,78.5180851733,1238.6870851733,2024-06-01,2024-06-30
intelligenttrialsprecompute,aws-green R&D,767904627276,Amazon ElastiCache,3572.905,3572.905,2024-06-01,2024-06-30
intelligenttrialsprecompute,aws-green R&D,767904627276,Amazon Elastic Container Service,0,3572.905,2024-06-01,2024-06-30
itpaapi,aws-green R&D,767904627276,Amazon ElastiCache,70.176,159.4467221821,2024-06-01,2024-06-30
itpaapi,aws-green R&D,767904627276,Amazon Elastic Container Service,0,159.4467221821,2024-06-01,2024-06-30
itpaapi,aws-green R&D,767904627276,Amazon Elastic Load Balancing,38.9236581679,159.4467221821,2024-06-01,2024-06-30
itpaapi,aws-green R&D,767904627276,Amazon Relational Database Service,48.7319912624,159.4467221821,2024-06-01,2024-06-30
itpaapi,aws-green R&D,767904627276,Amazon Simple Storage Service,1.6150727518,159.4467221821,2024-06-01,2024-06-30
itsfstack,aws-green R&D,767904627276,AWS Lambda,5759.1991810655,5764.8756754992,2024-06-01,2024-06-30
itsfstack,aws-green R&D,767904627276,Amazon Simple Storage Service,5.6764944337,5764.8756754992,2024-06-01,2024-06-30
operationalanalytics,aws-green R&D,767904627276,AWS Shield,0.0021244337,343.4827638507,2024-06-01,2024-06-30
operationalanalytics,aws-green R&D,767904627276,Amazon ElastiCache,77.928,343.4827638507,2024-06-01,2024-06-30
operationalanalytics,aws-green R&D,767904627276,Amazon Elastic Container Service,0,343.4827638507,2024-06-01,2024-06-30
operationalanalytics,aws-green R&D,767904627276,Amazon Elastic Load Balancing,90.3339905254,343.4827638507,2024-06-01,2024-06-30
operationalanalytics,aws-green R&D,767904627276,Amazon Relational Database Service,175.2186488916,343.4827638507,2024-06-01,2024-06-30
pa-model-services,aws-green R&D,767904627276,AWS Lambda,258.208883677,1919.5082370348,2024-06-01,2024-06-30
pa-model-services,aws-green R&D,767904627276,Amazon SageMaker,1661.233333276,1919.5082370348,2024-06-01,2024-06-30
pa-model-services,aws-green R&D,767904627276,AmazonCloudWatch,0.0660200818,1919.5082370348,2024-06-01,2024-06-30
patientdsapi,aws-green R&D,767904627276,Amazon ElastiCache,186.328,3087.2531593504,2024-06-01,2024-06-30
patientdsapi,aws-green R&D,767904627276,Amazon Elastic Container Service,0,3087.2531593504,2024-06-01,2024-06-30
patientdsapi,aws-green R&D,767904627276,Amazon Elastic Load Balancing,78.9520521814,3087.2531593504,2024-06-01,2024-06-30
patientdsapi,aws-green R&D,767904627276,Amazon MQ,17.0514193405,3087.2531593504,2024-06-01,2024-06-30
patientdsapi,aws-green R&D,767904627276,Amazon Relational Database Service,2804.9216878285,3087.2531593504,2024-06-01,2024-06-30
performsfapi,aws-green R&D,767904627276,AWS Shield,0.0063229893,7806.3083836414,2024-06-01,2024-06-30
performsfapi,aws-green R&D,767904627276,Amazon ElastiCache,7441.058,7806.3083836414,2024-06-01,2024-06-30
performsfapi,aws-green R&D,767904627276,Amazon Elastic Container Service,0,7806.3083836414,2024-06-01,2024-06-30
performsfapi,aws-green R&D,767904627276,Amazon Elastic Load Balancing,106.8553397494,7806.3083836414,2024-06-01,2024-06-30
performsfapi,aws-green R&D,767904627276,Amazon Relational Database Service,258.3194196032,7806.3083836414,2024-06-01,2024-06-30
performsfapi,aws-green R&D,767904627276,Amazon Simple Storage Service,0.0693012995,7806.3083836414,2024-06-01,2024-06-30
scdvisualizer,aws-green R&D,767904627276,Amazon ElastiCache,9.741,9.741,2024-06-01,2024-06-30
studyfeasibility,aws-green R&D,767904627276,Amazon ElastiCache,12851.244,31392.5183005243,2024-06-01,2024-06-30
studyfeasibility,aws-green R&D,767904627276,Amazon OpenSearch Service,332.2341892413,31392.5183005243,2024-06-01,2024-06-30
studyfeasibility,aws-green R&D,767904627276,Amazon SageMaker,18209.040111283,31392.5183005243,2024-06-01,2024-06-30
edgesfui,aws-red,565378680304,Amazon Elastic Container Service,0,38.8358464713,2024-06-01,2024-06-30
edgesfui,aws-red,565378680304,Amazon Elastic Load Balancing,38.8358464713,38.8358464713,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-red,565378680304,AWS AppSync,2.127604,2648.6192016372,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-red,565378680304,AWS Lambda,2645.0739849372,2648.6192016372,2024-06-01,2024-06-30
intelligent-trials-capabilities,aws-red,565378680304,Amazon Simple Queue Service,1.4176127,2648.6192016372,2024-06-01,2024-06-30
intelligent-trials-model-services,aws-red,565378680304,Amazon SageMaker,469.3506667176,469.3506667176,2024-06-01,2024-06-30
intelligent-trials-model-services-extension,aws-red,565378680304,AWS Lambda,137.6915909721,137.6915909721,2024-06-01,2024-06-30
intelligenttrialsapi,aws-red,565378680304,Amazon ElastiCache,0,26.0992362421,2024-06-01,2024-06-30
intelligenttrialsapi,aws-red,565378680304,Amazon Elastic Container Service,0,26.0992362421,2024-06-01,2024-06-30
intelligenttrialsapi,aws-red,565378680304,Amazon Elastic Load Balancing,26.0992362421,26.0992362421,2024-06-01,2024-06-30
intelligenttrialscapabilities,aws-red,565378680304,Amazon ElastiCache,713.958,787.7989722491,2024-06-01,2024-06-30
intelligenttrialscapabilities,aws-red,565378680304,Amazon Relational Database Service,73.8409722491,787.7989722491,2024-06-01,2024-06-30
intelligenttrialsprecompute,aws-red,565378680304,Amazon Elastic Container Service,0,0,2024-06-01,2024-06-30
itpaapi,aws-red,565378680304,Amazon ElastiCache,0,273.0148216553,2024-06-01,2024-06-30
itpaapi,aws-red,565378680304,Amazon Elastic Container Service,0,273.0148216553,2024-06-01,2024-06-30
itpaapi,aws-red,565378680304,Amazon Elastic Load Balancing,25.8324303554,273.0148216553,2024-06-01,2024-06-30
itpaapi,aws-red,565378680304,Amazon Relational Database Service,247.1823912999,273.0148216553,2024-06-01,2024-06-30
itsfstack,aws-red,565378680304,AWS Lambda,3457.488271516,3458.0388846374,2024-06-01,2024-06-30
itsfstack,aws-red,565378680304,Amazon Simple Storage Service,0.5506131214,3458.0388846374,2024-06-01,2024-06-30
operationalanalytics,aws-red,565378680304,Amazon ElastiCache,68.187,106.9393646637,2024-06-01,2024-06-30
operationalanalytics,aws-red,565378680304,Amazon Elastic Container Service,0,106.9393646637,2024-06-01,2024-06-30
operationalanalytics,aws-red,565378680304,Amazon Elastic Load Balancing,38.7523646637,106.9393646637,2024-06-01,2024-06-30
pa-model-services,aws-red,565378680304,AWS Lambda,258.1804681821,1918.5087059491,2024-06-01,2024-06-30
pa-model-services,aws-red,565378680304,Amazon SageMaker,1660.2674999427,1918.5087059491,2024-06-01,2024-06-30
pa-model-services,aws-red,565378680304,AmazonCloudWatch,0.0607378243,1918.5087059491,2024-06-01,2024-06-30
patientdsapi,aws-red,565378680304,Amazon Elastic Container Service,0,706.6665062059,2024-06-01,2024-06-30
patientdsapi,aws-red,565378680304,Amazon Elastic Load Balancing,26.2029700282,706.6665062059,2024-06-01,2024-06-30
patientdsapi,aws-red,565378680304,Amazon MQ,34.1033873652,706.6665062059,2024-06-01,2024-06-30
patientdsapi,aws-red,565378680304,Amazon Relational Database Service,646.3601488125,706.6665062059,2024-06-01,2024-06-30
performsfapi,aws-red,565378680304,Amazon ElastiCache,38.964,233.6183672888,2024-06-01,2024-06-30
performsfapi,aws-red,565378680304,Amazon Elastic Container Service,0,233.6183672888,2024-06-01,2024-06-30
performsfapi,aws-red,565378680304,Amazon Elastic Load Balancing,38.8073954426,233.6183672888,2024-06-01,2024-06-30
performsfapi,aws-red,565378680304,Amazon Relational Database Service,155.7596585018,233.6183672888,2024-06-01,2024-06-30
performsfapi,aws-red,565378680304,Amazon Simple Storage Service,0.0873133444,233.6183672888,2024-06-01,2024-06-30
performsfapibeta,aws-red,565378680304,Amazon ElastiCache,38.964,346.8905270476,2024-06-01,2024-06-30
performsfapibeta,aws-red,565378680304,Amazon Elastic Container Service,0,346.8905270476,2024-06-01,2024-06-30
performsfapibeta,aws-red,565378680304,Amazon Elastic Load Balancing,12.9055750647,346.8905270476,2024-06-01,2024-06-30
performsfapibeta,aws-red,565378680304,Amazon Relational Database Service,295.0209519829,346.8905270476,2024-06-01,2024-06-30
scdvisualizer,aws-red,565378680304,AWS Shield,0.0066499388,25.83065651,2024-06-01,2024-06-30
scdvisualizer,aws-red,565378680304,Amazon ElastiCache,0,25.83065651,2024-06-01,2024-06-30
scdvisualizer,aws-red,565378680304,Amazon Elastic Container Service,0,25.83065651,2024-06-01,2024-06-30
scdvisualizer,aws-red,565378680304,Amazon Elastic Load Balancing,25.8240065712,25.83065651,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,AWS Shield,0.0048262733,140.3131135641,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,Amazon ElastiCache,0,140.3131135641,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,Amazon Elastic Load Balancing,25.8436832072,140.3131135641,2024-06-01,2024-06-30
scdvisualizerbeta,aws-red,565378680304,Amazon Relational Database Service,114.4646040836,140.3131135641,2024-06-01,2024-06-30
studyfeasibility,aws-red,565378680304,Amazon ElastiCache,9638.433,9638.433,2024-06-01,2024-06-30
"""

# Read data into a DataFrame
df = pd.read_csv(StringIO(data))

# Create a unique product_id
df['product_id'] = df['product'].astype('category').cat.codes

# Create product DataFrame
product_df = df[['product_id', 'product','total_product_cost']].drop_duplicates().reset_index(drop=True)

# Create service DataFrame
service_df = df[['service', 'account_id', 'cost', 'product_id']]
print("**************product******************")
print(product_df)

print("*********service***********************")
print(service_df)
# Create tables in PostgreSQL and insert data
# product_df.to_sql('product', engine, if_exists='replace', index=False)
# service_df.to_sql('service', engine, if_exists='replace', index=False)

print("Data has been inserted into PostgreSQL tables.")
