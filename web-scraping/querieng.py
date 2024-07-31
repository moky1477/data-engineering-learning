import sqlite3
import pandas as pd

sql_connection = sqlite3.connect('web-scraping/ScrapedMovies.db')

csv_path = 'web-scraping/Queried_df.csv'

query = """

    SELECT * from Top_50_movies_post_1980
    WHERE Film = 'Batman';

"""
# data_dict = {
#     'Average Rank': [101],
#     'Film': ['Batman'],
#     'Year': [2014]
# }
# data_append = pd.DataFrame(data_dict)

# data_append.to_sql('Top_50_movies_post_1980', sql_connection, if_exists = 'append', index =False)
# print('Data appended successfully')

df = pd.read_sql(query, sql_connection)
df.to_csv(csv_path, index=False)

sql_connection.close()