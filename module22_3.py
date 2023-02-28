# 2_2.3: Used to generate a monthly bill for a credit card number for a given month and year.

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip

# Initialize Spark Session
sp = SparkSession.builder.appName("transactions").getOrCreate()

df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", "root") \
    .option("password", "password") \
    .load()


pd_credit = df_sp_cc.toPandas()
list_cc = list(pd_credit['CUST_CC_NO'])


def validate_cc(var_cc, list_cc):
    if var_cc in list_cc:
        return True
    else:
        if var_cc != 0:
            print("Not a valid Creditcard Number. Try again or enter 0 to exit")            
        return False

def show_info(df_sp_cc, var_cc, var_month, var_year):
    print(var_cc, str(var_month).rjust(2,'0'), str(var_year))
    result_cc = df_sp_cc.where((df_sp_cc['CUST_CC_NO'] == var_cc) & (df_sp_cc['TIMEID'].substr(5,2) == str(var_month).rjust(2,'0')) & \
                               (df_sp_cc['TIMEID'].substr(1,4) == str(var_year)))
    result_total = df_sp_cc.where((df_sp_cc['CUST_CC_NO'] == var_cc) & (df_sp_cc['TIMEID'].substr(5,2) == str(var_month).rjust(2,'0')) & \
                               (df_sp_cc['TIMEID'].substr(1,4) == str(var_year))).agg(sum(df_sp_cc['TRANSACTION_VALUE']).alias('TOTAL'))
    
    print('Credit Car No : ************{}'.format(var_cc[12:]))
    result_cc['TRANSACTION_TYPE', 'TRANSACTION_VALUE'].show()
    result_total.show()
    

def test_call_3(df_sp_cc, list_cc):
    while True:
        var_cc = pyip.inputStr("Enter 16-digit Creditcard Number (0 to Exit) : ")
    
        if validate_cc(var_cc, list_cc):
            
            while True:
                var_month = pyip.inputInt("Enter Billing Month (0 to Exit) : ") 
                if var_month in range(0,13):
                    break
                else:
                    print("Enter valid month in MM format (from 01 - 12) : ")
                continue
            
            if var_month == 0:
                break

            while True:
                var_year = pyip.inputInt("Enter Billing Year (0 to Exit) : ")
                # print(year(current_date))
                if var_year == 0 or var_year in range(1900,2023):
                    break
                else:
                    print("Enter a valid year between 1900 and 2023. 0 to Exit!")
                    continue
            if var_year == 0:
                break

            show_info(df_sp_cc, var_cc, var_month, var_year)
        else:
            if var_cc == '0':
                break
            continue

# test_call_3(df_sp_cc, list_cc)
# 4210653349028689 05 2018