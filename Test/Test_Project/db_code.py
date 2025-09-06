import sqlite3
import pandas as pd
conn = sqlite3.connect('STAFF.db') 
table_name = 'INSTRUCTOR'
attribute_list = ['ID', 'FNAME', 'LNAME', 'CITY', 'CCODE']

# Load the CSV file into a DataFrame
file_path = 'INSTRUCTOR.csv'
df = pd.read_csv(file_path, names = attribute_list)


# Write the data to a sqlite table

df.to_sql(table_name, conn, if_exists = 'replace', index =False)
print('Table is ready')


#Python Scripting: Running basic queries on data

# Viewing all the data in the table.
query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

# Viewing only FNAME column of data
query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)

#Viewing the total number of entries in the table.
query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement, conn)
print(query_statement)
print(query_output)


# creeate the dataframe of new data to be appended
data_dict ={'ID': [100],
            'FNAME': ['John'],
            'LNAME': ['Doe'],
            'CITY': ['New York'],
            'CCODE': ['USA']}
data_append = pd.DataFrame(data_dict)
print(data_append)

# Append the new data to the instructor table
data_append.to_sql(table_name, conn, if_exists='append', index=False)
print('New data appended')


conn.close()
