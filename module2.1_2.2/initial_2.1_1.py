import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import pymysql

# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()
# CUST_CC_NO, transaction_type, transaction_value

def show_report(df_sp_cc_cust, zip, yr, mm):
    print("inside the function", zip, yr, mm)
    data = df_sp_cc_cust.select('TRANSACTION_ID','transaction_type', \
                                'transaction_value', 'Date').where((df_sp_cc_cust['CUST_ZIP'] == zip) \
                                & (substring(df_sp_cc_cust['TIMEID'],1,4) == str(yr)) \
                                & (substring(df_sp_cc_cust['TIMEID'],5,2) == str(mm).rjust(2,'0')))
    data.show()


# 2.1 (1) Display the transactions made by customers living in a given zip code for a given month and year. 
# Order by day in descending order.

while True:
    query = "(SELECT cc.*, cust.cust_zip \
        FROM cdw_sapp_credit_card as cc \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN) as a"

    df_sp_cc_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query) \
    .option("user", "root") \
    .option("password", "password") \
    .load()

    df_sp_cc_cust = df_sp_cc_cust.withColumn('Date', concat(df_sp_cc_cust['TIMEID'].substr(0,4), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(5,2), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(7,2) \
                                               ))
    
    zip = pyip.inputInt("Enter 5-digit zipcode: ")
    if zip == 0:
        break
    if len(str(zip)) != 5:
        print("Enter a valid 5-digit zipcode (0 to Exit) : ")
        continue
    
    yr = 2023
    while ~(yr < 1900 or yr >= 2023):
        yr = pyip.inputInt("Enter year: ")
        # print(year(current_date))
        if yr == 0 or yr in range(1900,2023):
            break
        else:
            print("Enter a valid year between 1900 and 2023. 0 to Exit!")
            continue
    
    if yr == 0:
        break

    mm = pyip.inputInt("Enter month: ")
    if mm < 1 or mm > 12:
        continue
    show_report(df_sp_cc_cust, zip, yr, mm)
    
    
