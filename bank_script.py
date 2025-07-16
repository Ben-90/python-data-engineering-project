# Code for ETL operations on Country-GDP data

# Importing the required libraries

import requests 
import pandas as pd 
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime 

csv_path = "file.csv"


def log_progress(message):
    with open("logfile.txt", "a") as f:
        f.write(f"{datetime.now()} : {message}\n")


def extract(url, table_attribs):
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')

    # Trouver la première table du contenu (celle avec les banques)
    table = soup.find('table', {'class': 'wikitable'})
    rows = table.find_all('tr')

    df = pd.DataFrame(columns=table_attribs)

    for row in rows[1:]: 
        cells = row.find_all('td')
        if len(cells) >= 3:
            try:
                name = cells[1].find('a')['title']
                market_cap = cells[2].text.strip().replace('$', '')\
                    .replace('billion', '').strip()
                df.loc[len(df)] = [name, float(market_cap)]
            except (IndexError, TypeError, ValueError):
                continue 

    return df


def transform(df, csv_path):
    
    df['MC_EUR_Billion'] = df['MC_USD_Billion'] * 0.93    
    df['MC_GBP_Billion'] = df['MC_USD_Billion'] * 0.80     
    df['MC_INR_Billion'] = df['MC_USD_Billion'] * 83.10 
    df.to_csv(csv_path)
    return df


def create_table(sql_connection, table_name):
    cur = sql_connection.cursor()

    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INTEGER PRIMARY KEY AUTOINCREMENT, 
            Name TEXT, 
            MC_USD_Billion REAL NOT NULL, 
            MC_EUR_Billion REAL NOT NULL, 
            MC_GBP_Billion REAL NOT NULL, 
            MC_INR_Billion REAL NOT NULL
        )
        """
    )

    return sql_connection.commit()

##########################################################

def load_to_csv(df, file_name_csv):
    df.to_csv(file_name_csv)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final data frame to a database
    table with the provided name. Function returns nothing.'''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the query on the database table and
    prints the output on the terminal. Function returns nothing. '''
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    rows = cursor.fetchall()

    for row in rows:
        print(row)

''' Here, you define the required entities and call the relevant
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''


######################################################################


######################################################################


log_progress("initialisation of ETL ")
table_attribs = ['Name', 'MC_USD_Billion']
url = "https://en.wikipedia.org/wiki/List_of_largest_banks" 
df1 = extract(url, table_attribs)
print(transform(df1, csv_path))
load_to_csv(df1, "file_finale.csv")
log_progress("Fin de l'ETL")
log_progress(" loading and creating the csv file")

log_progress("connection to our database")
sql_connection = sqlite3.connect("banque_a.db")

log_progress(" verification of database connection")
if sql_connection:
     print("connection reussi")
else: 
     print("la connection a mal tournée")

# Création de la table
log_progress("Table creating")
table1 = "banques_table" 
create_table(sql_connection, table1)
table2 = "banques2"
create_table(sql_connection, table2)


log_progress("data load to Database as table.")
# # Charger les données dans la base de données
load_to_db(df1, sql_connection, table1)

log_progress("Execute queries")
print("\nTop 5 banques :")
run_query(f"SELECT Name, MC_USD_Billion FROM {table1} ORDER BY MC_USD_Billion DESC LIMIT 5", sql_connection)

log_progress("processus complete")
# # Fermer la connexion à la base
sql_connection.close()
log_progress("server connection close")
