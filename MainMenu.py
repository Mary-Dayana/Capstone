# Main Menu File

import pyinputplus as pyip
import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import 2_module21_1
import 2_module21_2
import 2_module21_3
import 2_module22_1
import 2_module22_2
import 2_module22_3
import 2_module22_4
import warnings

warnings.filterwarnings('ignore')
# Creating Spark Session
sp = SparkSession.builder.appName("Customer").getOrCreate()

df_sp_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CUSTOMER") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", "root") \
    .option("password", "password") \
    .load()

query = "(SELECT cc.*, cust.cust_zip \
        FROM cdw_sapp_credit_card as cc \
        JOIN cdw_sapp_customer as cust ON cc.CUST_SSN = cust.SSN) as a"

df_sp_cc_cust = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query) \
    .option("user", "root") \
    .option("password", "password") \
    .load()

query1 = "(SELECT bc.BRANCH_CODE, bc.BRANCH_STATE,\
      cc.TRANSACTION_VALUE,cc.TRANSACTION_TYPE \
      FROM cdw_sapp_branch bc \
      JOIN cdw_sapp_credit_card cc ON bc.BRANCH_CODE = cc.BRANCH_CODE) as b"

df_sp_br_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", query1) \
    .option("user", "root") \
    .option("password", "password") \
    .load()

df_sp_cc_cust = df_sp_cc_cust.withColumn('Date', concat(df_sp_cc_cust['TIMEID'].substr(0,4), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(5,2), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(7,2) \
                                               ))
df_pd_cust = df_sp_cust.toPandas()
list_ssn = list(df_pd_cust['SSN'])

pd_credit = df_sp_cc.toPandas()
list_cc = list(pd_credit['CUST_CC_NO'])

df_pd_cc = df_sp_cc.toPandas()
list_type = list(df_pd_cc['TRANSACTION_TYPE'].drop_duplicates())


list_main_menu = ["2.1.1: Transactions made by customers by Zipcode",
                  "2.1.2: Count and total values of transactions for a given type",
                  "2.1.3: Total number and total values of transactions for branches in a given state",
                  "2.2.1: Check the existing account details of a customer",
                  "2.2.2: Modify the existing account details of a customer",
                  "2.2.3: Generate a monthly bill for a credit card number for a given month and year",
                  "2.2.4: Display the transactions made by a customer between two dates",
                  "Exit"]


while True:
    var_main_menu = pyip.inputMenu(list_main_menu, numbered=True)
    print(var_main_menu)
    # if var_main_menu == 'Exit':
        # break
    if var_main_menu == '2.1.1: Transactions made by customers by Zipcode':
        2_module21_1.test_call_1(df_sp_cc_cust)
    elif var_main_menu == '2.1.2: Count and total values of transactions for a given type':
        2_module21_2.test_call_6(df_sp_cc)
    elif var_main_menu == '2.1.3: Total number and total values of transactions for branches in a given state':
        2_module21_3.test_call_7()   
    elif var_main_menu == '2.2.1: Check the existing account details of a customer':
        2_module22_1.test_call_4()
    elif var_main_menu == '2.2.2: Modify the existing account details of a customer':
        2_module22_2.edit_info()
    elif var_main_menu == '2.2.3: Generate a monthly bill for a credit card number for a given month and year':
        2_module22_3.test_call_3(df_sp_cc, list_cc)   
    elif var_main_menu == '2.2.4: Display the transactions made by a customer between two dates':
        2_module22_4.test_call_2(df_sp_cc_cust, list_ssn)
    else:
        break
