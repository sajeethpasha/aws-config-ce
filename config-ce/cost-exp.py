# Importing the StringIO module.
from io import StringIO 
import pandas as pd
import s3fs

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

def getCeDf(bucketName,fileName):
  try:
      s3 = boto3.resource('s3')
     
      obj = s3.Object(bucketName,fileName)
      n = obj.get()['Body'].read()
      gzipfile = BytesIO(n)
      gzipfile = gzip.GzipFile(fileobj=gzipfile)
      content = gzipfile.read()
      #  printD('content',content)
      s=str(content,'utf-8')
      TESTDATA = StringIO(s)
      df  = pd.read_csv(TESTDATA, sep=",")
      printD('content',df)
  except Exception as e:
      print(e)
      raise e
    

def unzipCeDf(bucketName,fileName):
    try:
     s3 = boto3.resource('s3')
     obj = s3.Object(bucketName,fileName)
     printD('obj',obj)
     n = obj.get()['Body'].read()
     gzipfile = BytesIO(n)
     gzipfile = gzip.GzipFile(fileobj=gzipfile)
     content = gzipfile.read()
     return content         
    except Exception as e:
        print(e)
        raise e


def getCostExpDfs():

    client = boto3.client('s3')
    # bucket = client.get_bucket(BUCKET_NAME)
    response  = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix='cost//sample-data-export/data/BILLING_PERIOD=2024-05/', Delimiter = '/')

    printD('response',response)
    ceDfs=[]
    for dt in response['CommonPrefixes']:
     filePath = dt['Prefix']
     printD('date',filePath)
     file = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=filePath, Delimiter = '/')
     fls = file['Contents']
     for fl in fls:
      #    printD('fl',fl)
      contentBytes=unzipCeDf(BUCKET_NAME,fl['Key'])
      #    printD('fileBytes',contentBytes)
      s=str(contentBytes,'utf-8')
      TESTDATA = StringIO(s)
      df  = pd.read_csv(TESTDATA, sep=",")
      ceDfs.append(df)
      printD('added',1)
    return ceDfs
      #    printD('content',df)
    
      #   for date in dates['cont']:
      #          datePath=date['Prefix']
      #          print(datePath)
    
  
  # fl = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=date, Delimiter = '/')
  # printD('fl',fl)
  
  

  
# ================================================================    
# BUCKET_NAME = "akhil-s3-bucket-760"
# fileName='cost//sample-data-export/data/BILLING_PERIOD=2024-05/2024-05-18T20:16:37.306Z-3695a728-d559-40bd-ac30-ffc97b552489/sample-data-export-00001.csv.gz'

BUCKET_NAME = "sample-143"
fileName='sample-data-export-00001.csv'


# getCeDf(BUCKET_NAME,key)
  
# prefix = 'cost//sample-data-export/data/BILLING_PERIOD=2024-05/'  
# ceDfs=getCostExpDfs()
# printD('size',len(ceDfs ) )
# client = boto3.client('s3')
# file = client.list_objects_v2(Bucket=BUCKET_NAME,Prefix=fileName, Delimiter = '/')
# printD('file',file)
# filPath = file['Prefix']

s3 = boto3.resource('s3')
obj = s3.Object(BUCKET_NAME,fileName)
printD('obj',obj)
n = obj.get()['Body'].read()
     
# contentBytes=unzipCeDf(BUCKET_NAME,fileName)
# printD('contentBytes',contentBytes)
s=str(n,'utf-8')
TESTDATA = StringIO(s)
df  = pd.read_csv(TESTDATA, sep=",")  
printD('df',df)
printD('df size:',len(df) )
# for fl in fls:
#  #    printD('fl',fl)
#  contentBytes=unzipCeDf(BUCKET_NAME,fl['Key'])
#  #    printD('fileBytes',contentBytes)
#  s=str(contentBytes,'utf-8')
#  TESTDATA = StringIO(s)
#  df  = pd.read_csv(TESTDATA, sep=",")  

