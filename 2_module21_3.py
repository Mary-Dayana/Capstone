# 2_1.3: Used to display the total number and total values of transactions for branches in a given state.


import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, DoubleType
import pyinputplus as pyip
import pandas.io.sql as psql

# Initialize Spark Session
sp = SparkSession.builder.appName("transactions").getOrCreate()


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


# def show_report(state):
   
    # data = df_sp_br_cc.select('BRANCH_STATE', \
    #                           'TRANSACTION_VALUE').where(df_sp_br_cc['BRANCH_STATE'] == state) \
    #                           .groupby('BRANCH_STATE').agg(sum('TRANSACTION_VALUE'))   
    # data.show(1)        

    # data = df_sp_br_cc.select('Branch_code','transaction_value').where( df_sp_br_cc['BRANCH_STATE']== state).groupby('branch_state')\
    # .agg(count('branch_code')),round(sum('transaction_value'),2).show()



df_br_cc = df_sp_br_cc.toPandas()
list_branch = list(df_br_cc['BRANCH_STATE'])

def test_call_7():
        while True:
            state = pyip.inputStr("Enter State : ")

            if state in list_branch:
                df_sp_br_cc.where(df_sp_br_cc['BRANCH_STATE'] == state)\
                    .groupBy('BRANCH_CODE').agg(count(df_sp_br_cc['TRANSACTION_VALUE']).alias('NUMBER OF TRANSACTIONS'),
                    round(sum(df_sp_br_cc['TRANSACTION_VALUE']),2).alias('TRANSACTION VALUE')).sort('BRANCH_CODE').show()

                # show_report(state)
                # data.show()
            else:
                break
        
# test_call_7()