import requests
import numpy as np 
import pandas as pd 
from datetime import datetime
from bs4 import BeautifulSoup
import sqlite3
import re

bmw_url = 'https://www.bmw.in/en/all-models.html'
merc_url = 'https://www.mbusa.com/en/all-vehicles'
table_attributes = ['Brand_Name', 'Car_Name', 'Car_Type', 'Base_model_price']
db_name = 'ETL_Luxury_Car_Data_Extraction/Luxury_cars.db'
table_name = 'Luxury_cars_table'
csv_path = 'ETL_Luxury_Car_Data_Extraction/Luxury_cars.csv'
sql_connection = sqlite3.connect(db_name)
combined_attributes = [merc_url, bmw_url, table_attributes, db_name, csv_path, sql_connection]

def extract(merc_url, bmw_url, table_attributes):
    '''
    This function extracts the required
    information from the website and saves it to a dataframe.
    The function returns the dataframe for further processing.
    '''

    def extract_bmw(bmw_url, table_attributes):
        df_bmw = pd.DataFrame(columns=table_attributes)
        html_page = requests.get(bmw_url).text
        data = BeautifulSoup(html_page, 'html.parser')

        car_elements = data.find_all('div', class_='cmp-modelcard')

        rows = []
        for car_element in car_elements:
            car_name = car_element.find('h5', class_='cmp-modelcard__name')
            car_name = car_name.text.strip() if car_name else 'N/A'
            
            car_type = car_element.find('div', class_='cmp-modelcard__fuel-type')
            car_type = car_type.span.text.strip() if car_type and car_type.span else 'N/A'
            
            starting_money = car_element.find('span', class_='cmp-modelcard__price')
            starting_money = starting_money.text.strip() if starting_money else 'N/A'

            rows.append({'Brand_Name': 'BMW', 'Car_Name': car_name, 'Car_Type': car_type, 'Base_model_price': starting_money})
        
        df_bmw = pd.DataFrame(rows, columns=table_attributes)
        return df_bmw
    
    def extract_merc(merc_url, table_attributes):
        df_merc = pd.DataFrame(columns=table_attributes)
        html_page = requests.get(merc_url).text
        data = BeautifulSoup(html_page, 'html.parser')

        car_elements = data.find_all('a', class_='all-vehicles__class-page-link')

        rows = []
        for car_element in car_elements:
            car_name = car_element.find('h3', class_='all-vehicles__class-name')
            car_name = car_name.text.strip() if car_name else 'N/A'

            car_price = car_element.find('h4', class_='all-vehicles__class-price')
            if car_price:
                # Extract text and remove unwanted parts
                car_price_text = car_price.text.strip()
                car_price = car_price.find('span')
                car_price = car_price.text.strip() if car_price else car_price_text
            else:
                car_price = 'N/A'
            
            rows.append({'Brand_Name': 'Mercedes', 'Car_Name': car_name, 'Car_Type': 'Not mentioned', 'Base_model_price': car_price})

        df_merc = pd.DataFrame(rows, columns=table_attributes)
        return df_merc
    
    df_bmw = extract_bmw(bmw_url, table_attributes)
    df_merc = extract_merc(merc_url, table_attributes)

    df = pd.concat([df_bmw, df_merc], ignore_index=True)

    return df

def transform(df):
    '''
    This function applies transformation on the extracted
    data. It includes data cleaning, adding/ removing new columns.
    Basically all the pre-processing.
    '''

    pattern_price = r'[^0-9.,]'
    pattern_name = r'[^a-zA-Z0-9 ]'

    df['Base_model_price'] = df['Base_model_price'].apply(lambda x: re.sub(pattern_price, '', x))
    df['Car_Name'] = df['Car_Name'].apply(lambda x: re.sub(pattern_name, '', x))

    df['Base_model_price'] = df['Base_model_price'].replace('', 'Not Available')
    df['Base_model_price'] = df['Base_model_price'].replace('N/A', 'Not Available')

    df['Base_model_price'] = df['Base_model_price'].str.replace(',', '', regex=False)

    df['Base_model_price'] = df['Base_model_price'].apply(lambda x: float(x) if pd.notna(x) and x.strip() != '' and x.replace('.', '', 1).isdigit() else np.nan)
    df['Base_model_price'] = df['Base_model_price'].apply(lambda x: x*83.77 if pd.notna(x) and x < 500000 else x)
    
    return df

def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)

def load_to_db(df, sql_connection, table_name):
    df.to_sql(name=table_name, con=sql_connection, if_exists='replace', index=False)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. '''
    timestamp_format = '%Y-%m-%d %H:%M:%S' # Year-Month-Day Hour:Minute:Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("ETL_Luxury_Car_Data_Extraction/etl_project_log.txt", "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

def execution_pipeline(combined_attributes):
    '''
    Streamline all the calling of above defined functions. 
    Also log the progress while calling functions.
    '''
    log_progress('Preliminaries complete. Initiating ETL process')
    df = extract(merc_url, bmw_url, table_attributes)
    log_progress('Data extraction complete. Initiating Transformation process')
    df = transform(df)
    log_progress('Data transformation complete. Initiating loading process')
    load_to_csv(df, csv_path)
    log_progress('Data Saved to csv file')
    load_to_db(df, sql_connection, table_name)
    log_progress('Data loaded to Database as table')
    log_progress('Process Complete')
    sql_connection.close()

if __name__ == '__main__':
    execution_pipeline(combined_attributes)
