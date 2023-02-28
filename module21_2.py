import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyinputplus as pyip

# Creating Spark Session
sp = SparkSession.builder.appName("transactions").getOrCreate()

# Read Loan data from Database
df_sp_cc = sp.read.format("jdbc") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "CDW_SAPP_CREDIT_CARD") \
    .option("user", "root") \
    .option("password", "password") \
    .load()


# 2_1.2: Used to display the number and total values of transactions for a given type

def transaction_type_show(trans_type):

    data = df_sp_cc.select('transaction_type', 'transaction_value').where(df_sp_cc['TRANSACTION_TYPE']==trans_type)\
            .groupby('transaction_type').agg(sum('transaction_value'), count('transaction_type'))
        
    data.show(1)

df_pd_cc = df_sp_cc.toPandas()
list_type = list(df_pd_cc['TRANSACTION_TYPE'].drop_duplicates())

def test_call_6(df_sp_cc):   
    while True:
        trans_type = pyip.inputStr("Enter Transaction_Type : ")

        # list_type_menu = ["1. Bills",
        #                   "2. Grocery",
        #                   "3. Education",
        #                   "4. Entertainment",
        #                   "5. Healthcare",
        #                   "6. Gas",
        #                   "7. Test",
        #                  ]

        if trans_type in list_type:
            transaction_type_show(trans_type)
        else:
            break

# test_call_6(df_sp_cc)