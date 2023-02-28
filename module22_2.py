# 2_2.1: Used to modify the existing account details of a customer.

import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import pymysql

# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()

df_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_pd_cust = df_cust.toPandas()
list_ssn = list(df_pd_cust['SSN'])
# list_ssn

list_col = ['FIRST_NAME', 'MIDDLE_NAME', 'LAST_NAME', 'CUST_EMAIL', 'CUST_PHONE', 'FULL_STREET_ADDRESS', 'CUST_CITY', \
                'CUST_STATE', 'CUST_ZIP', 'EXIT']

def validate_ssn(var_ssn):
    if var_ssn in list_ssn:
        return True
    else:
        if var_ssn != 0:
            print("Not a valid SSN. Try again or enter 0 to exit")            
        return False

def validate_ans(var_ans):
    if var_ans == 'Y' :
        return True
    else:
        if var_ans != 'N':
            print("Enter Y to edit or N to exit")            
        return False

def edit_info():
    while True:
        var_ssn = pyip.inputInt("Enter 9-digit SSN : ")
        if validate_ssn(var_ssn):
            result = df_cust.where(df_cust['SSN'] == var_ssn)
            break
        else:
            if var_ssn == 0:
                break
            continue

    pd_result = result.toPandas()
    while True:
        # 123457849
        var_option = pyip.inputMenu(list_col, numbered=True)
        
        if var_option == "FIRST_NAME": 
            print(f"Existing value : {pd_result.loc[0,'FIRST_NAME']}")
            var_first_name = pyip.inputStr("Enter new FIRST NAME : ")
            pd_result.loc[0,'FIRST_NAME']= var_first_name
            print(pd_result.loc[0,'FIRST_NAME'])
        elif var_option == "MIDDLE_NAME": 
            print(f"Existing value : {pd_result.loc[0,'MIDDLE_NAME']}")
            var_middle_name = pyip.inputStr("Enter new MIDDLE NAME : ")
            pd_result.loc[0,'MIDDLE_NAME']= var_middle_name
            print(pd_result.loc[0,'MIDDLE_NAME'])
        elif var_option == "LAST_NAME": 
            print(f"Existing value : {pd_result.loc[0,'LAST_NAME']}")
            var_last_name = pyip.inputStr("Enter new LAST NAME : ")
            pd_result.loc[0,'LAST_NAME'] = var_last_name
            print(pd_result.loc[0,'LAST_NAME'])
        elif var_option == "CUST_EMAIL": 
            print(f"Existing value : {pd_result.loc[0,'CUST_EMAIL']}")
            var_email = pyip.inputEmail("Enter new EMAIL : ")
            pd_result.loc[0,'CUST_EMAIL'] = var_email
            print(pd_result.loc[0,'CUST_EMAIL'])
        elif var_option == "CUST_PHONE": 
            print(f"Existing value : {pd_result.loc[0,'CUST_PHONE']}")
            var_phone = pyip.inputInt("Enter new PHONE (xxx)xxx-xxxx : ")
            if len(str(var_phone)) != 10:
                print("Invalid Phone Number!")
                continue
            pd_result.loc[0,'CUST_PHONE'] = '('+str(var_phone)[0:3] + ')'+ str(var_phone)[3:6] + '-' + str(var_phone)[6:]
            print(pd_result.loc[0,'CUST_PHONE'])
        elif var_option == "FULL_STREET_ADDRESS":
            print(f"Existing value : {pd_result.loc[0,'FULL_STREET_ADDRESS']}")
            var_address = pyip.inputStr("Enter new ADDRESS : ")
            pd_result.loc[0,'FULL_STREET_ADDRESS'] = var_address
            print(pd_result.loc[0,'FULL_STREET_ADDRESS'])
        elif var_option == "CUST_CITY": 
            print(f"Existing value : {pd_result.loc[0,'CUST_CITY']}")
            var_city = pyip.inputStr("Enter new CITY : ")
            pd_result.loc[0,'CUST_CITY'] = var_city
            print(pd_result.loc[0,'CUST_CITY'] )
        elif var_option == "CUST_STATE": 
            print(f"Existing value : {pd_result.loc[0,'CUST_STATE']}")
            var_state = pyip.inputStr("Enter new STATE : ")
            pd_result.loc[0,'CUST_STATE'] = var_state
            print(pd_result.loc[0,'CUST_STATE'])
        elif var_option == "CUST_ZIP": 
            print(f"Existing value : {pd_result.loc[0,'CUST_ZIP']}")
            var_zip = pyip.inputZip("Enter new ZIPCODE : ")
            pd_result.loc[0,'CUST_ZIP'] = var_zip
            print(pd_result.loc[0,'CUST_ZIP'])
        elif var_option == 'EXIT':
            upd_query = "UPDATE cdw_sapp_customer SET \
FIRST_NAME = '{}', MIDDLE_NAME = '{}', LAST_NAME = '{}', CUST_EMAIL = '{}', \
CUST_PHONE = '{}', FULL_STREET_ADDRESS = '{}', CUST_CITY = '{}', \
CUST_STATE = '{}', CUST_ZIP = {} WHERE SSN = {}".format(pd_result.loc[0, 'FIRST_NAME'], \
pd_result.loc[0, 'MIDDLE_NAME'], pd_result.loc[0, 'LAST_NAME'], pd_result.loc[0, 'CUST_EMAIL'], \
pd_result.loc[0, 'CUST_PHONE'], pd_result.loc[0, 'FULL_STREET_ADDRESS'], pd_result.loc[0, 'CUST_CITY'], \
pd_result.loc[0, 'CUST_STATE'], pd_result.loc[0, 'CUST_ZIP'], var_ssn)
            print(upd_query)
            
            try:
                connection = pymysql.connect(
                host='localhost',
                user='root',
                password='password',
                database='creditcard_capstone')
                print("Database connection successful!")

                cursor = connection.cursor()
                cursor.execute(upd_query)
                print("Record updated successfully!!")
                connection.commit()
                cursor.close()
                connection.close()
            except:
                print("Database Connection error. Update failed!")
            break

# edit_info()