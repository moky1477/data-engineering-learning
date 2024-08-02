import requests
from datetime import datetime
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np

# Initialising all the known entities
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'ETL_Global_GDP_Extraction/World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = 'ETL_Global_GDP_Extraction/Countries_by_GDP.csv'
sql_connection = sqlite3.connect(db_name)
combined_attributes = [url, table_attribs, csv_path, sql_connection, table_name]

def extract(url, table_attribs):
    ''' 
    This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. 
    '''
        
    df = pd.DataFrame(columns=table_attribs)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[2].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col):
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {
                    'Country': col[0].a.contents[0],
                    'GDP_USD_millions': col[2].contents[0]
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)

    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    df['GDP_USD_millions'] = df['GDP_USD_millions'].str.replace(',', '').astype(float)
    df['GDP_USD_billions'] = df['GDP_USD_millions']/1000
    df.drop('GDP_USD_millions', axis=1, inplace=True)
    df['GDP_USD_billions'] = df['GDP_USD_billions'].round(2)

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)
    

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    
    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

    ''' Here, you define the required entities and call the relevant 
    functions in the correct order to complete the project. Note that this
    portion is not inside any function.'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("ETL_Global_GDP_Extraction/etl_project_log.txt","a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def execution_pipeline(all_required_attributes):
    log_progress('Preliminaries complete. Initiating ETL process')
    df = extract(url, table_attribs)
    log_progress('Data extraction complete. Initiating Transformation process')
    df = transform(df)
    log_progress('Data transformation complete. Initiating loading process')
    load_to_csv(df, csv_path)
    log_progress('Data Saved to csv file')
    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table. Running the query')
    query = f"SELECT * from {table_name} WHERE GDP_USD_billions >= 100"
    run_query(query, sql_connection)
    log_progress('Process Complete')
    sql_connection.close()

if __name__ == '__main__':
    execution_pipeline(combined_attributes)