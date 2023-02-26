import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
# import pymysql

# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()

df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone")  \
    .option("dbtable", "cdw_sapp_credit_card") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

# df_sp_cc.createOrReplaceTempView('df_sp_cc_view')

pd_credit = df_sp_cc.toPandas()
list_cc = list(pd_credit['CUST_CC_NO'])

def validate_cc(var_cc):
    if var_cc in list_cc:
        return True
    else:
        if var_cc != 0:
            print("Not a valid Creditcard Number. Try again or enter 0 to exit")            
        return False

def show_info(var_cc, var_month, var_year):
    print(var_cc, str(var_month).rjust(2,'0'), str(var_year))
    result_cc = df_sp_cc.where((df_sp_cc['CUST_CC_NO'] == var_cc) & (df_sp_cc['TIMEID'].substr(5,2) == str(var_month).rjust(2,'0')) & \
                               (df_sp_cc['TIMEID'].substr(1,4) == str(var_year)))
    result_total = df_sp_cc.where((df_sp_cc['CUST_CC_NO'] == var_cc) & (df_sp_cc['TIMEID'].substr(5,2) == str(var_month).rjust(2,'0')) & \
                               (df_sp_cc['TIMEID'].substr(1,4) == str(var_year))).agg(sum(df_sp_cc['TRANSACTION_VALUE']).alias('TOTAL'))
    
    print('Credit Car No : ************{}'.format(var_cc[12:]))
    result_cc['TRANSACTION_TYPE', 'TRANSACTION_VALUE'].show()
    result_total.show()
    

    # result_cc = df_sp_cc.where((df_sp_cc['CUST_CC_NO'] == '4210653394681244') & (df_sp_cc['TIMEID'] == '20180430') \
    #                            (df_sp_cc['TIMEID'] == '20180401'))
    # result_mm = pd_credit.where(pd_credit['CUST_CC_NO'] == var_cc).sum(pd_credit['TRANSACTION_VALUE'])
    # print(result_mm.show())
    # result_cc['TRANSACTION_TYPE', 'TRANSACTION_VALUE'].show()

    # total_view = sp.sql("SELECT cust_cc_no as Credit Card #, \
    #     SUBSTR(timeid,1,4) AS 'Billing Year', \
    #     SUBSTR(timeid,5,2) AS 'Billing Month', \
    #     SUM(transaction_value) \
    #     FROM df_sp_cc_view \
    #     WHERE (cust_cc_no = var_cc) \
    #     AND (SUBSTR(timeid,1,4) = str(var_month)) \
    #     AND (SUBSTR(timeid,5,2) = str(var_year)) ")
    # total_view.show()

    # cc_view = sp.sql("SELECT transaction_type, transaction_value \
    #     FROM df_sp_cc_view  \
    #     WHERE (cust_cc_no = var_cc) \
    #     AND (SUBSTR(timeid,1,4) = str(var_month)) \
    #     AND (SUBSTR(timeid,5,2) = str(var_year))")
    # cc_view.show()

    


# print(df_sp_cc.show())

while True:
    var_cc = pyip.inputStr("Enter 16-digit Creditcard Number : ")
    var_month = 1
    var_year = 2018
    if validate_cc(var_cc):
        var_month = pyip.inputInt("Enter Billing Month : ")
        if var_month < 1 or var_month > 12:
            print("Enter valid month in MM format (from 01 - 12) : ") 
            continue
        var_year = pyip.inputInt("Enter Billing Year : ")

        show_info(var_cc, var_month, var_year)
    else:
        if var_cc == '0':
            break
        continue


