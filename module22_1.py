# 2_2.1: Used to check the existing account details of a customer.

import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import pymysql

# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()

df_sp_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_pd_cust = df_sp_cust.toPandas()
list_ssn = list(df_pd_cust['SSN'])
# list_ssn

# result = df_cust.where(df_cust['SSN'] == '123452342')

# df_pd_cust.loc[df_pd_cust.loc[:,'SSN'] == 123452342,:].head()
# result.printSchema()
# print(result.show())
# print(result.collect()[0][7])
# var = 'FIRST NAME : ' + result.collect()[0][7]
# print(var)

#var_ssn = 123452342

def validate_ssn(var_ssn):
    if var_ssn in list_ssn:
        return True
    else:
        if var_ssn != 0:
            print("Not a valid SSN. Try again or enter 0 to exit")            
        return False

def show_info(var_ssn):
    result = df_sp_cust.where(df_sp_cust['SSN'] == var_ssn)
    
    result['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CUST_EMAIL', 'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_CITY', \
                'CUST_STATE', 'CUST_ZIP', 'CREDIT_CARD_NO'].show()
   
def test_call_4():
    while True:
        var_ssn = pyip.inputInt("Enter 9-digit SSN : ")
    
        if validate_ssn(var_ssn):
            show_info(var_ssn)
            break
        else:
            if var_ssn == 0:
                break
        # continue

# test_call_4()
# 123456100
# 123453023