import pandas as pd
import os
import glob
import psycopg2 

DB_HOST = "localhost"
DB_NAME = "amazon_data"
DB_USER = "postgres"
DB_PASS = "admin"
PORT = 5433

# WORKS EXACTLY HOW I WANT IT TO
table_names_list = []
for i in glob.glob("C:/Users/teric/Desktop/sql_learning/amazon_data/*.csv"):
    #first get name of csv file
    print(i)
    char1 = "data"
    char2 = '.csv'
    mystr = i
    table_name = mystr[mystr.find(char1)+5 : mystr.find(char2)].replace(" ", "_").lower() + "_raw"
    print(table_name)
    print(table_name)
    table_names_list.append(table_name)

    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=PORT)
    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS RAWamazon.{table_name}; CREATE TABLE RAWamazon.{table_name} (name VARCHAR, main_category VARCHAR, sub_category VARCHAR, image VARCHAR, link VARCHAR, ratings VARCHAR, no_of_ratings VARCHAR, discount VARCHAR, actual_price VARCHAR);")
    conn.commit()
    
    cur.execute(f"COPY RAWamazon.{table_name} FROM '{i}' DELIMITER ',' CSV HEADER;")
    conn.commit()


    cur.execute(f"UPDATE RAWamazon.{table_name} SET actual_price = REPLACE(actual_price, '₹', ''); \
                UPDATE RAWamazon.{table_name} SET actual_price = REPLACE(actual_price, ',', ''); \
                UPDATE RAWamazon.{table_name} SET discount = REPLACE(discount, '₹', ''); \
                UPDATE RAWamazon.{table_name} SET discount = REPLACE(discount, ',', ''); \
                UPDATE RAWamazon.{table_name} SET no_of_ratings = REPLACE(no_of_ratings, ',', ''); \
                UPDATE RAWamazon.{table_name} SET no_of_ratings = NULL WHERE (no_of_ratings ~* '[a-z]') is true;\
                UPDATE RAWamazon.{table_name} SET ratings = NULL WHERE (ratings ~* '[a-z]') is true; \
                UPDATE RAWamazon.{table_name} SET ratings = REPLACE(ratings, '₹', ''); \
                ALTER TABLE RAWamazon.{table_name}\
                ALTER COLUMN actual_price SET DATA TYPE DECIMAL USING actual_price::DECIMAL;\
                ALTER TABLE RAWamazon.{table_name} \
                ALTER COLUMN discount SET DATA TYPE DECIMAL USING discount::DECIMAL; \
                ALTER TABLE RAWamazon.{table_name} \
                ALTER COLUMN ratings SET DATA TYPE DECIMAL USING ratings::DECIMAL; \
                ALTER TABLE RAWamazon.{table_name} \
                ALTER COLUMN no_of_ratings SET DATA TYPE INT USING no_of_ratings::INT;")


    conn.commit()
    conn.close()


loop_df = pd.DataFrame()
for j in table_names_list:
    print(j)
    conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=PORT)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM RAWamazon.{j};")
    conn.commit()
    result = cur.fetchall()
    conn.close()
    result_df = pd.DataFrame(result)
    frames = [loop_df, result_df]
    loop_df = pd.concat(frames)


loop_df.columns = ['name', 'main_category', 'sub_category', 'image', 'link', 'ratings', 'no_of_ratings', 'discount_rupees', 'actual_price_rupess']
print(loop_df)
print(type(loop_df))
loop_df.to_csv("C:/Users/teric/Desktop/sql_learning/all_amazon_data.csv")


conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=PORT)
cur = conn.cursor()
cur.execute(f"DROP TABLE IF EXISTS final_amazon.all_amazon_data; CREATE TABLE final_amazon.all_amazon_data (index VARCHAR, name VARCHAR, main_category VARCHAR, sub_category VARCHAR, image VARCHAR, link VARCHAR, ratings DECIMAL, no_of_ratings DECIMAL, discount_rupees DECIMAL, actual_price_rupees DECIMAL);")
conn.commit()

cur.execute(f"COPY final_amazon.all_amazon_data FROM 'C:/Users/teric/Desktop/sql_learning/all_amazon_data.csv' DELIMITER ',' CSV HEADER;")
conn.commit()

cur.execute(f"ALTER TABLE final_amazon.all_amazon_data\
            ALTER COLUMN no_of_ratings SET DATA TYPE INT USING no_of_ratings::INT;")
conn.commit()
conn.close()


