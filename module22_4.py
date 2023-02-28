# 2_1.4: Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip

# Initialize Spark Session
sp = SparkSession.builder.appName("transactions").getOrCreate()

query = "(SELECT cc.*, cust.cust_zip \
        FROM cdw_sapp_credit_card as cc \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN) as a"

df_sp_cc_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query) \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_sp_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_sp_cc_cust = df_sp_cc_cust.withColumn('Date', concat(df_sp_cc_cust['TIMEID'].substr(0,4), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(5,2), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(7,2) \
                                               ))

df_pd_cust = df_sp_cust.toPandas()
list_ssn = list(df_pd_cust['SSN'])

def show_info(df_sp_cc_cust, var_ssn, var_start_dt, var_end_dt):
    result = df_sp_cc_cust.select('TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'Date').where((df_sp_cc_cust['CUST_SSN'] == str(var_ssn)) \
                            & (df_sp_cc_cust['Date'] >= var_start_dt) \
                            & (df_sp_cc_cust['Date'] <= var_end_dt))
    
    result.sort(desc('TIMEID')).show()

def validate_ssn(var_ssn, list_ssn):
    if var_ssn in list_ssn:
        return True
    else:
        if var_ssn != 0:
            print("Not a valid SSN. Try again or enter 0 to exit")            
        return False
    
def test_call_2(df_sp_cc_cust, list_ssn):
    while True:
        var_ssn = pyip.inputInt("Enter 9-digit SSN (0 to Exit) : ")
        
        if validate_ssn(var_ssn, list_ssn):
            var_start_dt = pyip.inputDate("Enter Start Date in (YYYY/MM/DD) format : ")
            var_end_dt = pyip.inputDate("Enter End Date in (YYYYMMDD) format : ")
            print("SSN - *****{} : Start Date - {} : End Date - {}".format(str(var_ssn)[5:], var_start_dt, var_end_dt))
            show_info(df_sp_cc_cust, var_ssn, var_start_dt, var_end_dt)
        else:
            if var_ssn == 0:
                break
            continue

# test_call_2(df_sp_cc_cust, list_ssn)
# 123453023
# Start Date - 2018-03-12 : End Date - 2018-05-12
