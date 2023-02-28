# Main Menu File

import pyinputplus as pyip
# import module222 as m
# from module222 import edit_info
import pyspark as py
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip
import module21_1
import module22_3
import module22_4

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

df_sp_cc_cust = df_sp_cc_cust.withColumn('Date', concat(df_sp_cc_cust['TIMEID'].substr(0,4), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(5,2), lit('-'), \
                                               df_sp_cc_cust['TIMEID'].substr(7,2) \
                                               ))
df_pd_cust = df_sp_cust.toPandas()
list_ssn = list(df_pd_cust['SSN'])

pd_credit = df_sp_cc.toPandas()
list_cc = list(pd_credit['CUST_CC_NO'])


list_main_menu = ["Transactions made by customers by Zipcode",
                  "Count and total values of transactions for a given type",
                  "Total number and total values of transactions for branches in a given state",
                  "Check the existing account details of a customer"
                  "Modify the existing account details of a customer",
                  "Generate a monthly bill for a credit card number for a given month and year",
                  "Display the transactions made by a customer between two dates",
                  "Data Analysis and Visualization",
                  "Exit"]


while True:
    var_main_menu = pyip.inputMenu(list_main_menu, numbered=True)
    print(var_main_menu)
    if var_main_menu == 'Exit':
        break
    elif var_main_menu == 'Transactions made by customers by Zipcode':
        module21_1.test_call(df_sp_cc_cust)
    elif var_main_menu == 'Display the transactions made by a customer between two dates':
        module22_4.test_call2(df_sp_cc_cust, list_ssn)
    elif var_main_menu == 'Generate a monthly bill for a credit card number for a given month and year':
        module22_3.test_call3(df_sp_cc, list_cc)

    # var_ssn = pyip.inputInt("Enter SSN : ")
    # # var_ans
    # if m.validate_ssn(df_pd_cust, var_ssn):
    #     m.edit_info(df_cust, var_ssn)
    #     print('Back from edit menu')
    #     continue
    # else:
    #     if var_ssn == 'N':
    #         break
    #     continue
