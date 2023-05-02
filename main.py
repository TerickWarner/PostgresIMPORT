import pandas as pd
import os
import glob
import psycopg2 

DB_HOST = "localhost"
DB_NAME = "amazon_data"
DB_USER = "postgres"
DB_PASS = "admin"
PORT = 5433

# test_name = "did_this_work"

# conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=PORT)

# cur = conn.cursor()

# cur.execute(f"CREATE TABLE amazontest.{test_name} (id SERIAL PRIMARY KEY, name VARCHAR);")
# conn.commit()

# conn.close()

for i in glob.glob("C:/Users/teric/Desktop/sql_learning/amazon_data/*.csv"):
    #first get name of csv file
    char1 = "data"
    char2 = '.csv'
    mystr = i
    table_name = mystr[mystr.find(char1)+5 : mystr.find(char2)].replace(" ", "_").lower() + "_TESTING"
    print(table_name)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=PORT)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS amazontest.{table_name}; CREATE TABLE amazontest.{table_name} (name VARCHAR, main_category VARCHAR, sub_category VARCHAR, image VARCHAR, link VARCHAR, ratings VARCHAR, no_of_ratings VARCHAR, discount VARCHAR, actual_price VARCHAR);")
    conn.commit()
    conn.close()

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=PORT)
    cur = conn.cursor()
    cur.execute(f"COPY amazontest.{table_name} FROM '{i}' DELIMITER ',' CSV HEADER;")
    conn.commit()
    conn.close()
    break


    # csv = pd.read_csv(i)
    # print(csv)
    # break

# char1 = "data"
# char2 = '.csv'
# mystr = "C:/Users/teric/Desktop/sql_learning/amazon_data\Jewellery.csv"
# print(mystr[mystr.find(char1)+5 : mystr.find(char2)])








#DELETE THE ROWS WITH GET IN RATINGS COLUMN 
# -- SELECT * FROM amazontest.air_conditioners_testing

# -- ALTER TABLE amazontest.air_conditioners_testing
# -- ALTER COLUMN ratings TYPE float USING ratings::float;

# DELETE FROM amazontest.air_conditioners_testing
# WHERE ratings = 'Get'; 
# ALTER TABLE amazontest.air_conditioners_testing
# ALTER COLUMN ratings TYPE float USING ratings::float;
# SELECT * FROM amazontest.air_conditioners_testing

#TRY IF A COLUMN CONTAINS A-Z make it null THIS MAKES A NEW COLUMN NAMED CASE
# SELECT ratings,
# CASE 
# 	WHEN ratings = '4.2' THEN 'TESTING'
# END
# FROM amazontest.air_conditioners_testing; 

# UPDATE amazontest.air_conditioners_testing
# SET ratings = 'testing' WHERE (ratings ~* '[a-z]') is true;
# Select * FROM amazontest.air_conditioners_testing;


#UPDATE RATINGS / NO OF RATINGS COLUMNS TO GET RID OF ANY LETTERS BEFORE CHANGING IT TO A FLOAT
# UPDATE amazontest.air_conditioners_testing
# SET ratings = 'testing' WHERE (ratings ~* '[a-z]') is true;
# UPDATE amazontest.air_conditioners_testing
# SET no_of_ratings = 'testing' WHERE (ratings ~* '[a-z]') is true;
# Select * FROM amazontest.air_conditioners_testing;

