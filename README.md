# Capstone
Mary_Capstone

Introduction:

    ETL process for a Loan Application dataset and a Credit Card dataset: 
    Using Python (Pandas, advanced modules e.g., Matplotlib), MariaDB, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. 

Extracting Transforming and Loading

    Step 1:

        Extract data from below Json files in to Dataframes
        1. CDW_SAPP_BRANCH.JSON
        2. CDW_SAPP_CREDITCARD.JSON
        3. CDW_SAPP_CUSTOMER.JSON
        4. Extract Data from Given API 

    Step 2:
        Trasform the data as per the requirements in the dataframes

    step 3: 
        Load the clean data in to database(Maria DB)  “creditcard_capstone”
        and tables accordingly
        CDW_SAPP_BRANCH
        CDW_SAPP_CREDIT_CARD
        CDW_SAPP_CUSTOMER 
        CDW-SAPP_loan_application 


Created a console-based Python program to satisfy System Requirements

    Transaction Details 
        1)    Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
        2)    Used to display the number and total values of transactions for a given type.
        3)    Used to display the total number and total values of transactions for branches in a given state
    Customer Details
        1) Used to check the existing account details of a customer.
        2) Used to modify the existing account details of a customer.
        3) Used to generate a monthly bill for a credit card number for a given month and year.
        4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.

    completed the validations for the above module


Data analysis and Visualization
    As per the requirements, made visualizations for the business analyst to analyze and vizualise the data.


References:
    MariaDB Documentation
    https://mariadb.org/

    PySpark
    https://spark.apache.org/docs/latest/api/python/index.html

    Apache Spark - Spark SQL
    https://spark.apache.org/sql/

    Analyzing and Visualization 
    https://www.analyticsvidhya.com/blog/2021/08/understanding-bar-plots-in-python-beginners-guide-to-data-visualization/
