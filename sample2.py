import boto3
import gzip
import json
from io import BytesIO
import gzip
import pandas as pd
from io import StringIO 
import s3fs

# BUCKET_NAME = "sample-143"
# BUCKET_NAME = "mybucket-sample-760"
BUCKET_NAME = "json-bucket-sample-123"
s3 = boto3.client('s3')

def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')
    
def saveDftoS3bucket(bucketName,  df): 
   

    printD('df',df)
    bytes_to_write = df.to_csv(None).encode()
    printD('bytes_to_write',bytes_to_write)
    # df.to_csv(index = False, encoding='utf-8') # False: not include index
    print(df)  
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep=",", index=False)
    printD('csv_buffer',csv_buffer)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucketName, 'sample3.csv').put(Body=csv_buffer.getvalue())

print("hello");   
df = pd.DataFrame( [ [1, 1, 1], [2, 2, 2] ], columns=['a', 'b', 'c']) 
saveDftoS3bucket(BUCKET_NAME,df)