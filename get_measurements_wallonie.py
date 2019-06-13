import pandas as pd
import time
import bs4 as bs
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import re
import json
import psycopg2

# The website *Les voies hydrauliques* encodes station types with these strings
QUANTITY_CODES = {
    'precipitation': '0015',
    'debit': '1002',
    'hauteur': '1011',
}

CONNECTION_DETAILS = "dbname='meuse' user='postgres' password='password' host='localhost' port='5222'"

# local dir to store resulting CSV files
DIR_STATIONS = '/Users/rik/meuse_forecast/datasets/S/wallonie'

def get_stations_db(station_type):
    """
    Returns a Pandas dataframe of stations from the database.
    Parameters:
      station_type (string): key into QUANTITY_CODES
    """

    table_name = f"wallonie.station_{station_type}"
    # fields is a list of required column names
    # SQL will ORDER BY the first field
    # returned df will be indexed by the first field
    fields = ['code', 'name', 'river', 'x', 'y']
    columns = ", ".join(fields)


    conn = psycopg2.connect(CONNECTION_DETAILS)
    cursor = conn.cursor()
    print("connected to database meuse")

    print("start selecting stations")
    q = f"""
        SELECT {columns} FROM {table_name}
        ORDER BY {fields[0]} ASC
        LIMIT 10000;
        """
    cursor.execute(q)

    conn.commit()
    print("row(s) retrieved")

    stations_list = cursor.fetchall()
    stations_df = pd.DataFrame(columns=fields, data=stations_list)
    stations_df.set_index('code', inplace=True)

    # df = pd.concat([df, cursor.fetchall()])

    cursor.close()
    conn.close()
    print("connection closed")
    return stations_df

print(get_stations_db('precipitation'))
print(get_stations_db('hauteur'))
print(get_stations_db('debit'))
