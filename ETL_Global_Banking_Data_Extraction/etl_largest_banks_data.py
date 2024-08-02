from datetime import datetime
import requests
import pandas as pd
import numpy as np
import sqlite3
from bs4 import BeautifulSoup

table_attributes = ['Name', 'MC_USD_Billion']
url = 'https://web.archive.org/web/20230908091635%20/https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'ETL_Global_Banking_Data_Extraction/Banks.db'
table_name = 'ETL_Global_Banking_Data_Extraction/Largest_banks'
csv_path = 'ETL_Global_Banking_Data_Extraction/largest_banks_data.csv'
sql_connection = sqlite3.connect(db_name)
combined_attributes = [url, table_attributes, csv_path, sql_connection, table_name]

def log_progress(message):
    ''' This function logs the mentioned message of a given stage of the
    code execution to a log file. Function returns nothing'''

    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open('code_log.txt', "a") as f:
        f.write(timestamp + ' : ' + message + '\n')

def extract(url, table_attributes):
    ''' This function aims to extract the required
    information from the website and save it to a data frame. The
    function returns the data frame for further processing. '''

    df = pd.DataFrame(columns=table_attributes)
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')
    
    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    for row in rows:
        col = row.find_all('td')
        if len(col):
            bank_name = col[1].text.strip()
            market_cap = col[2].text.strip()
            data_dict = {
                'Name': bank_name,
                'MC_USD_Billion': market_cap
            }
            df1 = pd.DataFrame(data_dict, index=[0])
            df = pd.concat([df, df1], ignore_index=True)            

    return df

def transform(df):
    ''' This function accesses the CSV file for exchange rate
    information, and adds three columns to the data frame, each
    containing the transformed version of Market Cap column to
    respective currencies'''
    df_er = pd.read_csv('ETL_Global_Banking_Data_Extraction/exchange_rate.csv')
        # Extract exchange rates
    rates = df_er.set_index('Currency')['Rate']
    rate_gbp = rates['GBP']
    rate_inr = rates['INR']
    rate_eur = rates['EUR']

    df['MC_USD_Billion'] = pd.to_numeric(df['MC_USD_Billion'], errors='coerce')
    
    # Calculate new columns
    df['MC_GBP_Billion'] = np.round(df['MC_USD_Billion'] * rate_gbp, 2)
    df['MC_INR_Billion'] = np.round(df['MC_USD_Billion'] * rate_inr, 2)
    df['MC_EUR_Billion'] = np.round(df['MC_USD_Billion'] * rate_eur, 2)

    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final data frame as a CSV file in
    the provided path. Function returns nothing.'''
    df.to_csv(csv_path, index=False)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    
# def run_query(query_statement, sql_connection):
#     ''' This function runs the query on the database table and
#     prints the output on the terminal. Function returns nothing. '''
# ''' Here, you define the required entities and call the relevant
# functions in the correct order to complete the project. Note that this
# portion is not inside any function.'''

df = extract(url, table_attributes)
transform(df)
load_to_csv(df, csv_path)