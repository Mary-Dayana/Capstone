# Capstone
Mary_Capstone

README for ETL and Data Visualization Cpastone Project
    This file provides information about the ETL capstone project at bootcamp.

INTRODUCTION:
    ETL process for a Loan Application dataset and a Credit Card dataset: 
    Using Python (Pandas, advanced modules e.g., Matplotlib), MariaDB, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries. 

PURPOSE:
    This project gives the oppurtunity to demonstarte the knowledge acquired in the bootcamp.
   
DESCRIPTION:
    - This project is focused on developing ETL pipeline that Extracts data from different data sources, Transformation of data based on the requirements and load data into database.
    - This project uses python libraries like Pandas, Pyspark, Seaborn, Matplotlib, printinputplus etc.
    - Along with the ETL, this project provides a simple Menu Driven Front-End to view and modify the data, generate reports and graphs for Data Analysis and Visualization

        EXTARCT, TRANSFORM and LOAD Data, Menu Driven Console, Visualizations
        ---  STEP 1: ---
                Extract data from below Json files in to Dataframes
                1. CDW_SAPP_BRANCH.JSON
                2. CDW_SAPP_CREDITCARD.JSON
                3. CDW_SAPP_CUSTOMER.JSON
                4. Extract Data from Given API 
        ---  STEP 2: ---
                Transform the data as per the requirements in the dataframes(Pandas and Pyspark)
        ---  STEP 3: ---
                Load the clean data in to database(Maria DB)  “creditcard_capstone”
                and tables accordingly
                CDW_SAPP_BRANCH
                CDW_SAPP_CREDIT_CARD
                CDW_SAPP_CUSTOMER 
                CDW-SAPP_loan_application 
        ---  STEP 4: ---
                Created a console-based Python program to satisfy System Requirements
        ---  STEP 5: ---
                Transaction Details 
                    1)    Used to display the transactions made by customers living in a given zip code for a given month and year. Order by day in descending order.
                    2)    Used to display the number and total values of transactions for a given type.
                    3)    Used to display the total number and total values of transactions for branches in a given state
                Customer Details
                    1) Used to check the existing account details of a customer.
                    2) Used to modify the existing account details of a customer.
                    3) Used to generate a monthly bill for a credit card number for a given month and year.
                    4) Used to display the transactions made by a customer between two dates. Order by year, month, and day in descending order.
        ---  STEP 6: ------  STEP 6: ---
                    Validations for the above module
        ---  STEP 7: ---
                    Data analysis and Visualization
                    As per the requirements, made visualizations for the business analyst to analyze and vizualise the data.
                    Screenshots for all Visualizations are placed in the "Screenshots" folder.

        SCREENSHOTS for Visualizations:
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\3.1.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\3.2.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\3.3.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\5.1.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\5.2.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\5.3.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\5.4.png
                        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\Tableau Screenshot.png

RESULT:
        The ETL pipeline was successful.
        -- Verify-- Check in Database for the success of dataLoad (or) Read the data from Database into Dataframe

PROJECT STRUCTURE:
    
    ETL files:
                1_ETL_json_branch.ipynb
                1_ETL_json_credit.ipynb
                1_ETL_json_customer.ipynb
                4_ETL_loan_API.ipynb

    Menu Driven files:
                module21_1.py
                module21_2.py
                module21_3.py
                module22_1.py
                module22_2.py
                module22_3.py
                module22_4.py
                MainMenu.py (step 5)
                Menu_Transactions.py (step 7)

    Visualization files:
                3_visualization_Files.ipynb (jupyter file)
                5_visualization_API.ipynb   (jupyter file)
                functions_visualizations.ipynb (python file having functions for all visualizations)
                
HOW TO RUN PROJECT IN MACHINE:

    1. Install Python 3.10.9 version
    2. Install Maria DB, HeidiSQL(GUI)
    3. Import Pandas, Pyspark, numpy, Matplotlib, Seaborn, printinputplus libraries
    4. Jupyter notebook (to check individual steps..easy for debugging)
    5. Git (create repository and clone into local machine)
    
CONCLUSION:
        - This Project uses technologies to manage ETL process for a Loan Application data and a credit card dataset using python (Pandas, Pyspark, numpy, Matplotlib, Seaborn, printinputpluslibraries), MariaDB, Apache Spark (Core, Spark SQL).
        - Loan Application data API containing Customer onlineapplication from data(for loan eligibility).         

REFERENCES:

    PROJECT WORKFLOW:
        C:\Users\Learner_XZHCG217\Desktop\TEK-Dataengineering\Git\Capstone\Screenshots\Project_Workflow.png
    
    MariaDB Documentation
        https://mariadb.org/

    PySpark
        https://spark.apache.org/docs/latest/api/python/index.html

    Apache Spark - Spark SQL
        https://spark.apache.org/sql/

    Analyzing and Visualization 
        https://www.analyticsvidhya.com/blog/2021/08/understanding-bar-plots-in-python-beginners-guide-to-data-visualization/
