from bs4 import BeautifulSoup
import sqlite3
import pandas as pd
import requests
import os

url = 'https://web.archive.org/web/20230902185655/https://en.everybodywiki.com/100_Most_Highly-Ranked_Films'

# Define the path and name of the SQLite database
# db_dir = 'web-scraping'
db_name = 'ScrapedMovies.db'
# db_path = os.path.join(db_dir, db_name)
# os.makedirs(db_dir, exist_ok=True)

# Define the path and name for the CSV file
csv_file_name = 'top_50_movies.csv'
# csv_path = os.path.join(db_dir, csv_file_name)

df = pd.DataFrame(columns=['Average Rank', 'Film', 'Year'])
 
html_page = requests.get(url).text
data = BeautifulSoup(html_page, 'html.parser')

tables = data.find_all('tbody')
rows = tables[0].find_all('tr')

count = 0
for row in rows:
    if count<50:
        col = row.find_all('td')
        if len(col):
            year_text = col[2].contents[0].strip()
            if year_text.isdigit() and int(year_text)>1980:
                data_dict = {
                    'Average Rank': col[0].contents[0].strip(),
                    'Film': col[1].contents[0].strip(),
                    'Year': year_text
                }
                df1 = pd.DataFrame(data_dict, index=[0])
                df = pd.concat([df, df1], ignore_index=True)
                count+=1
    else:
        break

df.to_csv(csv_file_name, index=False)

conn = sqlite3.connect(db_name)
df.to_sql('Top_50_movies_post_1980', conn, if_exists='replace', index=False)
conn.close()