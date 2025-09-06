# Code for ETL operations on Country-GDP data

# Importing the required libraries
from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''
    with open('code_log.txt', 'a') as log_file:
        log_file.write(f"{datetime.now()}: {message}\n")

#declaring known values

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ['Name', 'MC_USD_Billion', 'MC_GBP_Billion', 'MC_EUR_Billion', 'MC_INR_Billion']
db_name = 'Banks.db'
table_name = 'Largest_banks'
csv_path = 'exchange_rate.csv'
output_csv_path = 'Largest_banks_data.csv'



log_progress("Preliminary setup done. Initializing ETL process.")

def extract(url, table_attribs):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''
    page = requests.get(url).text
    data = BeautifulSoup(page,'html.parser')
    df = pd.DataFrame(columns=table_attribs)
    
    # Find the correct table - look for table with "By market capitalization" header
    tables = data.find_all('table', {'class': 'wikitable'})
    
    # Use the first wikitable which contains the bank data
    if tables:
        rows = tables[0].find_all('tr')
        count = 0
        
        for row in rows[1:]:  # Skip header row
            if count >= 10:  # Get top 10 banks
                break
                
            col = row.find_all('td')
            if len(col) >= 3:
                # Extract bank name and market cap
                bank_name = col[1].get_text().strip()  # Bank name is in 2nd column
                market_cap = col[2].get_text().strip()  # Market cap is in 3rd column
                
                # Clean the market cap value (remove any non-numeric characters except decimal)
                market_cap = market_cap.replace(',', '').replace('$', '').replace(' ', '')
                
                if bank_name and market_cap and market_cap.replace('.', '').isdigit():
                    data_dict = {"Name": bank_name,
                                 "MC_USD_Billion": float(market_cap)}
                    df1 = pd.DataFrame(data_dict, index=[0])
                    df = pd.concat([df,df1], ignore_index=True)
                    count += 1
    
    return df

log_progress("Data extraction complete. Initializing transformation process.")


def transform(df, csv_path):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    exchange_df = pd.read_csv(csv_path)
    exchange_rates = exchange_df.set_index('Currency')['Rate'].to_dict()
    
    df['MC_GBP_Billion'] = [round(float(x) * exchange_rates['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [round(float(x) * exchange_rates['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [round(float(x) * exchange_rates['INR'], 2) for x in df['MC_USD_Billion']]
    return df

log_progress("Data transformation complete. Initializing load process.")


def load_to_csv(df, output_csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    try:
        df.to_csv(output_csv_path, index=False)
        print(f"Data successfully saved to {output_csv_path}")
    except Exception as e:
        print(f"Error saving CSV file: {e}")

log_progress("Data saved to CSV file.")


log_progress("SQL Connection initiated.")

sql_connection = sqlite3.connect(db_name)
def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''


    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)

log_progress("Data loaded to database. Executing query.")

def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    query_result = pd.read_sql_query(query_statement, sql_connection)
    print(query_result)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''

# Execute the ETL process
df = extract(url, table_attribs)
print(f"Extracted {len(df)} rows")
print(df.head())
df = transform(df, csv_path)
load_to_csv(df, output_csv_path)
load_to_db(df, sql_connection, table_name)

# Run sample queries
run_query(f"SELECT * FROM {table_name}", sql_connection)
run_query(f"SELECT avg(MC_USD_Billion) FROM {table_name}", sql_connection)
run_query(f"SELECT Name FROM {table_name} WHERE MC_USD_Billion > 500", sql_connection)

sql_connection.close()
log_progress("Server connection closed.")
log_progress("ETL process completed successfully.")