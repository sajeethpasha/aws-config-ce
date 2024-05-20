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


def getconfigDatacontent(bucketName,fileName):
    try:
        s3 = boto3.resource('s3')
        obj = s3.Object(bucketName,fileName)
        n = obj.get()['Body'].read()
        gzipfile = BytesIO(n)
        gzipfile = gzip.GzipFile(fileobj=gzipfile)
        content = gzipfile.read()
        return content
    
    except Exception as e:
        print(e)
        raise e
    
    
def getConfigDf(content):
    y = json.loads(content)
    # printD('y',y)
    configurationItems = y['configurationItems']
    # printD('configurationItems:',configurationItems)
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
        # resourceNameArray.append(config['resourceName'])
        # arnArray.append(config['ARN'])
            
    data={
    "AwsAccountId": awsAccountIdArray,
    "ConfigurationItemStatus": configurationItemStatusArray,
    "Resource_type": resourceTypeArray ,
    "Resource_Id": resourceIdArray,
    # "ResourceNameArray": resourceNameArray,
    # "ArnArray": arnArray            
    }     
    df = pd.DataFrame(data)
    return df  
  
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
    
    
def getAllConfigDefs(bucketName,fileBaseFolderPath):
    
    client = boto3.client('s3')
    files  = client.list_objects_v2(Bucket=bucketName,Prefix=fileBaseFolderPath, Delimiter = '/')
    printD('response',files)
    filesArray=files['Contents']
    configDfsArray = []
    for file in filesArray:
        # printD('file',file)
        fileName = file['Key']
        content= getconfigDatacontent(bucketName,fileName)
        configdf=getConfigDf(content)
        printD('configdf',configdf)
        configDfsArray.append(configdf)
        # printD('content',content)
    return configDfsArray    
  
# ==================Start of code =================
INPUT_BUCKET_NAME = "config-bucket-016266444216"
FILE_BASE_FOLDER_PATH='AWSLogs/016266444216/Config/ap-south-1/2024/5/16/ConfigHistory/'

OUT_BUCKET_NAME = 'json-bucket-sample-123'
OUT_FILE_NAME = 'sample-s3-file-16.csv' 


configDfsArray=getAllConfigDefs(INPUT_BUCKET_NAME,FILE_BASE_FOLDER_PATH)
printD('configDfsArray size',len(configDfsArray) )

mergedDfs= pd.concat(configDfsArray,ignore_index=True)
printD('mergedDfs',mergedDfs)

saveDftoS3bucket(OUT_BUCKET_NAME,OUT_FILE_NAME,mergedDfs)



   