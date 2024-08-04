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
    
    # Define table attributes for the dataframe
    table_attributes = ['Brand_Name', 'Car_Name', 'Car_Type', 'Base_model_price']

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
    return df

# 3. load_to_csv()

# 4. load_to_db()

# 5. run_query()

# 6. log_progress()

# 7. execution_pipeline():
'''
Streamline all the calling of above defined functions. 
Also log the progress while calling functions.
'''
df = extract(merc_url, bmw_url, table_attributes)
df = transform(df)
df.to_csv(csv_path, index=False)