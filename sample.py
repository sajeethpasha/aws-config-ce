import boto3
import gzip
import json
from io import BytesIO
import gzip

# BUCKET_NAME = "sample-143"
# BUCKET_NAME = "mybucket-sample-760"
BUCKET_NAME = "config-bucket-016266444216"
s3 = boto3.client('s3')
 
# bucket_resp = s3.list_buckets()

# for bucket in bucket_resp["Buckets"]
#   print(bucket)

# response = s3.list_objects_v2(Buckket=BUCKET_NAME)
# for bucket in response["Contents"]:
#     print(bucket)

print("hello")
client = boto3.client('s3')
# list_bucket=client.list_buckets()
# print(list_bucket)
# pretty_json = json.dumps(list_bucket, indent=4)
# print(pretty_json)

# bucket = client.get_bucket(BUCKET_NAME)
# folders = bucket.list('', '/')
# for folder in folders:
#     print(folder.name)

# ************Testing with sample-143 table ********************************
# response  = client.list_objects_v2(Bucket='sample-143', Delimiter = '/')
# print('----------------------------------------------------')
# print(response)
# print('----------------------------------------------------')
# for prefix in response['CommonPrefixes']:
#     print(prefix['Prefix'])
    # awsLogs = prefix['Prefix']
    

# response  = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix='AWSLogs/016266444216/Config/', Delimiter = '/')
# print('----------------------------------------------------')
# print(response)
# print('----------------------------------------------------')
# for prefix in response['CommonPrefixes']:
#     print(prefix['Prefix'])
#     configRegion = prefix['Prefix']
    
#     print('-----------------------------year-----------------------')
#     yrs= client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=configRegion, Delimiter = '/')
#     for yr in yrs['CommonPrefixes']:
#      yrPath=yr['Prefix']
#      print(yrPath)
    
#      print('-----------------------------months-----------------------')
#      mnths= client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=yrPath, Delimiter = '/')
#      for mnth in mnths['CommonPrefixes']:
#       mnthPath=mnth['Prefix']
#       print(mnthPath)
      
#       print('-----------------------------dates-----------------------')
#       dates= client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=mnthPath, Delimiter = '/')
#       for date in dates['CommonPrefixes']:
#        datePath=date['Prefix']
#        print(datePath)
       
#        print('-----------------------------day-----------------------')
#        dates= client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=mnthPath, Delimiter = '/')
#        for date in dates['CommonPrefixes']:
#          datePath=date['Prefix']
#          print(datePath)
     
    
    # AWSLogs/016266444216/Config/ap-south-1/2024/5/18/
    # json print
    
def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')
    
# fileLocation = 'AWSLogs/016266444216/Config/ap-south-1/2024/5/16/ConfigHistory/'    
# response  = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=fileLocation, Delimiter = '/')
# print('----------------------------------------------------')
# print(response)
# print('----------------------------------------------------')
# for content in response['Contents']:
#     # print(content)
#      dta=content['Key']
#      printD('data',dta)
#     #  fileName=fileLocation+dta
#      fileName ='AWSLogs/016266444216/Config/ap-south-1/2024/5/16/ConfigHistory/016266444216_Config_ap-south-1_ConfigHistory_AWS::Lambda::Function_20240516T023328Z_20240516T033012Z_1.json.gz'
#      printD('fileName :',fileName)
#      content_object = client.get_object(Bucket=BUCKET_NAME, Key=fileName)
#      printD('content_object :',content_object)
#      with gzip.GzipFile(fileobj=content_object.get()["Body"]) as gzipfile:
#       content = gzipfile.read()
#       print(content)
#      jsonStr= json.dump(content_object)
#      printD('jsonStr :',jsonStr)
    #  text = content_object["Body"].read().decode()
    #  printD('text :',text)
    #  file_content = dta.get()['Body'].read().decode('utf-8')
    #  printD("file_content",file_content)
    #  json_content = json.loads(file_content)
    #  printD("json_content",json_content)
     
    # configRegion = prefix['Prefix']
    
def saveToS3Bucket(fileName,data):    
    s3 = boto3.resource('s3')
    obj = s3.Object('json-bucket-sample-123', fileName)
    obj.put(Body=(bytes(json.dumps(data).encode('UTF-8'))))


try:
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
     saveToS3Bucket('akhil.json',configurationItems)
     for config in configurationItems:
         printD('resourceId',config['configurationItemVersion'])
         
except Exception as e:
    print(e)
    raise e


   
 
# response  = client.list_objects(Bucket=BUCKET_NAME,Prefix='AWSLogs/', Delimiter = '/')
# print('----------------------------------------------------')
# print(response)
# print('----------------------------------------------------')
# for prefix in response['CommonPrefixes']:
#     print(prefix['Prefix'])
#     # awsLogs = prefix['Prefix']  
  

# print(client.list_buckets())
# print(client.list_objects_v2())

# for bucket in client.list_buckets():
#     print(bucket)
    # my_bucket =client.Bucket(bucket.name)
    
    # for file in my_bucket.objects.all():
    #     print(f"bucket : {bucket.name}  key: {file.key}")
