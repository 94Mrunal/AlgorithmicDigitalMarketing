import snowflake.connector

cnn = snowflake.connector.connect(
    user='mrunal',
    password='Welcome24@',
    account='fza88194.us-east-1'
    )
cs = cnn.cursor()
try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
    print("Creating warehouse..")
    sq1 = "CREATE WAREHOUSE IF NOT EXISTS  project_warehouse_assignment"
    cs.execute(sq1)
    print("Creating database..")
    sq1 = "CREATE DATABASE IF NOT EXISTS project_database_assignment"
    cs.execute(sq1)
    print("Using Database")
    sq1 = "USE DATABASE project_database_assignment"
    cs.execute(sq1)
    print("Creating Schema..")
    sq1 = "CREATE SCHEMA IF NOT EXISTS project_schema_assignment"
    cs.execute(sq1)
    print('Creation complete..')
    sq1 = "USE WAREHOUSE project_warehouse_assignment"
    cs.execute(sq1)
    sq1 = "USE DATABASE project_database_assignment"
    cs.execute(sq1)
    sq1 = "USE SCHEMA project_schema_assignment"
    cs.execute(sq1)
    sq1 = ("CREATE OR REPLACE TABLE project_assignment"
           "(Clothing_ID integer, Age integer,Title string, Review_Text string, Rating integer,Recommended_IND integer,Positive_Feedback_Count integer, Division_Name string, Department_Name string, Class_Name string  )")
    cs.execute(sq1)

finally:
    cs.close()
cnn.close()