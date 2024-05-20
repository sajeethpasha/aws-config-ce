import boto3
import json

import gzip

from io import BytesIO
from io import StringIO 
import gzip
import pandas as pd


def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')
    
def saveToS3Bucket(fileName,data):    
    s3 = boto3.resource('s3')
    obj = s3.Object('json-bucket-sample-123', fileName)
    obj.put(Body=(bytes(json.dumps(data).encode('UTF-8'))))  
    
def saveDftoS3bucket(bucketName,fileName, df): 
    printD('df',df)
    bytes_to_write = df.to_csv(None).encode()
    printD('bytes_to_write',bytes_to_write)
    # df.to_csv(index = False, encoding='utf-8') # False: not include index
    print(df)  
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, sep=",", index=False)
    printD('csv_buffer',csv_buffer)
    s3_resource = boto3.resource("s3")
    s3_resource.Object(bucketName, fileName).put(Body=csv_buffer.getvalue())      

try:
     BUCKET_NAME = "config-bucket-016266444216"
     s3 = boto3.resource('s3')
     key='AWSLogs/016266444216/Config/ap-south-1/2024/5/16/ConfigHistory/016266444216_Config_ap-south-1_ConfigHistory_AWS::Lambda::Function_20240516T023328Z_20240516T033012Z_1.json.gz'
     obj = s3.Object(BUCKET_NAME,key)
     n = obj.get()['Body'].read()
     gzipfile = BytesIO(n)
     gzipfile = gzip.GzipFile(fileobj=gzipfile)
     content = gzipfile.read()
     printD('content',content)
     y = json.loads(content)
     printD('y',y)
     configurationItems = y['configurationItems']
     printD('configurationItems:',configurationItems)
    #  saveToS3Bucket('akhil.json',configurationItems)
     awsAccountIdArray = []
     configurationItemStatusArray = []
     resourceTypeArray = []
     resourceIdArray = []
     resourceNameArray = []
     arnArray = []
     for config in configurationItems:
         configItemVersion=config['configurationItemVersion']
        # // printD('resourceId',configItemVersion[''])
         awsAccountIdArray.append(config['awsAccountId'])
         configurationItemStatusArray.append(config['configurationItemStatus'])
         resourceTypeArray.append(config['resourceType'])
         resourceIdArray.append(config['resourceId'])
         resourceNameArray.append(config['resourceName'])
         arnArray.append(config['ARN'])
         
     data={
        "AwsAccountId": awsAccountIdArray,
        "ConfigurationItemStatus": configurationItemStatusArray,
        "ResourceTypeArray": resourceTypeArray ,
        "ResourceIdArray": resourceIdArray,
        "ResourceNameArray": resourceNameArray,
        "ArnArray": arnArray            
     }     
     df = pd.DataFrame(data)
     printD('df',df) 
     outBucketName = 'json-bucket-sample-123'
     saveDftoS3bucket(outBucketName,'akhibhai.csv',df)
              
         
except Exception as e:
    print(e)
    raise e
    