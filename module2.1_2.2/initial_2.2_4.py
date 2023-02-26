import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import pymysql
import datetime as dt

# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()

df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")  \
    .option("dbtable", "cdw_sapp_credit_card") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_pd_cust = df_sp_cc.toPandas()
list_ssn = list(df_pd_cust['CUST_SSN'])
list_ssn

from pyspark.sql.types import DateType
df_sp_cc = df_sp_cc.withColumn('Date', concat(df_sp_cc['TIMEID'].substr(0,4), lit('-'), \
                                               df_sp_cc['TIMEID'].substr(5,2), lit('-'), \
                                               df_sp_cc['TIMEID'].substr(7,2) \
                                               ))

def show_info(var_ssn, var_start_dt, var_end_dt):
    result = df_sp_cc.select('TRANSACTION_TYPE', 'TRANSACTION_VALUE', 'Date').where((df_sp_cc['CUST_SSN'] == str(var_ssn)) \
                            & (df_sp_cc['Date'] >= var_start_dt) \
                            & (df_sp_cc['Date'] <= var_end_dt))
    
    result.sort(desc('TIMEID')).show()

def validate_ssn(var_ssn):
    if var_ssn in list_ssn:
        return True
    else:
        if var_ssn != 0:
            print("Not a valid SSN. Try again or enter 0 to exit")            
        return False
    

while True:
    var_ssn = pyip.inputInt("Enter 9-digit SSN (0 to Exit) : ")
    print(var_ssn)
    
    if validate_ssn(var_ssn):
        var_start_dt = pyip.inputDate("Enter Start Date in (YYYY/MM/DD) format : ")
        # var_start_dt = (str(var_start_dt1)[0:4]) + (str(var_start_dt1)[5:7]) + (str(var_start_dt1)[8:])
        var_end_dt = pyip.inputDate("Enter End Date in (YYYYMMDD) format : ")
        # var_end_dt = (str(var_end_dt1)[0:4]) + (str(var_end_dt1)[5:7]) + (str(var_end_dt1)[8:])
        # var_ssn1 = substr(var_ssn, 5, 4)
        print("SSN - *****{} : Start Date - {} : End Date - {}".format(str(var_ssn)[5:], var_start_dt, var_end_dt))
        show_info(var_ssn, var_start_dt, var_end_dt)
        break
    else:
        if var_ssn == 0:
            break
        continue
while True:
    var_ssn = pyip.inputInt("Enter 9-digit SSN : ")
    var_ssn
    if validate_ssn(var_ssn):
        var_start_dt = pyip.inputInt("Enter Start Date in (YYYYMMDD) format : ")
        var_end_dt = pyip.inputInt("Enter End Date in (YYYYMMDD) format : ")
        print(var_ssn, var_start_dt, var_end_dt)
        show_info(var_ssn, var_start_dt, var_end_dt)
        break
    else:
        if var_ssn == 0:
            break
        continue