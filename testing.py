import pandas as pd
from datetime import datetime as d


def printD(name,object):
    print('-------------------'+name+'---------------------------------')
    print(object)
    print('-----------------------------------------------------')
    
date = d.now()
# print(date.strftime("%Y-%m-%d %H:%M:%S"))
print(date.year)
print(date.month)
print(date.day)

year = date.year
month = date.month
day = date.day

# 2024/5/16/

# AWSLogs/016266444216/Config/ap-south-1/2024/5/16/ConfigHistory/016266444216_Config_ap-south-1_ConfigHistory_AWS::Lambda::Function_20240516T023328Z_20240516T033012Z_1.json.gz'
key=f'AWSLogs/016266444216/Config/ap-south-1/{year}/{month}/{day}/ConfigHistory'
printD('key',key)


 
# Import date and timedelta class
# from datetime module
from datetime import date
from datetime import timedelta
 
# Get today's date
today = date.today()
print("Today is: ", today)
 
# Yesterday date
yesterday = today - timedelta(days = 1)
print("Yesterday was: ", yesterday)
