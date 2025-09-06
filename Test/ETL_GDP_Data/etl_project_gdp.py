# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime 

url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ["Country", "GDP_USD_millions"]
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'


log_file = "log_file.txt"
target_csv = "Countries_by_GDP_in_bn.csv"
db_name = 'World_Economies.db'

def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''
    
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')
    count = 0
    for row in rows:
        if count < 25:
            col = row.find_all('td')
            if len(col) >= 3:
                if col[0].find('a') is not None and '—' not in col[2].text:
                    data_dict = {"Country": col[0].a.contents[0],
                                 "GDP_USD_millions": col[2].contents[0]}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)
                    count += 1
    print(df)
    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    gdp_list =df["GDP_USD_millions"].tolist()
    gdp_list = [float(i.replace(',',''))/1000 for i in gdp_list]
    gdp_list = [round(i,2) for i in gdp_list]
    df["GDP_USD_Millions"] = gdp_list
    df = df.rename(columns={"GDP_USD_millions": "GDP_USD_billions"})
    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

    df.to_csv(csv_path, index=False)
    print("Data saved to CSV successfully.")

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    print("Data saved to database successfully.")


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    print(f"Running the query: {query_statement}")

    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    with open(log_file, "a") as f:
        f.write(f"{datetime.now()} - {message}\n")

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
df = extract(url, table_attribs)

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
df = transform(df)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_to_csv(df, csv_path)

sql_connection = sqlite3.connect(db_name)
load_to_db(df, sql_connection, table_name)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Log the completion of the ETL process
log_progress("ETL Job Ended")

# Running some basic queries on the database table
query_statement1= f"SELECT * FROM {table_name} WHERE GDP_USD_billions > 100 ORDER BY GDP_USD_billions DESC LIMIT 10"
query_statement2= f"SELECT AVG(GDP_USD_billions) FROM {table_name} WHERE GDP_USD_billions > 100"
run_query(query_statement1, sql_connection)
run_query(query_statement2 , sql_connection)

sql_connection.close()
