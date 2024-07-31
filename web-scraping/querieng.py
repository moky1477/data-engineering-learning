import sqlite3
import pandas as pd

sql_connection = sqlite3.connect('web-scraping\ScrapedMovies.db')

csv_path = 'web-scraping/Queried_df.csv'

query = """

    SELECT * from Top_50_movies_post_1980
    WHERE Film = 'Parasite';

"""

df = pd.read_sql(query, sql_connection)
df.to_csv(csv_path, index=False)