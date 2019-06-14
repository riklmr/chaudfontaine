import pandas as pd
import numpy as np
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

CONNECTION_DETAILS = "dbname='meuse' user='postgres' password='password' host='localhost' port='5333'"

UNKNOWN_FLOAT = -999999

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
# stations_precipitation = get_stations_db('precipitation')

# print(get_stations_db('precipitation'))
# print(get_stations_db('hauteur'))
# print(get_stations_db('debit'))

def build_url_StatHoraireTab(station_code, station_type, year, month):
    """
    Returns the URL for the correct page, given:
    station by code (string or integer)
    type of the station (string, key into QUANTITY_CODES)
    year (string or integer)
    month (string or integer)
    """
    url = 'http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do'
    url += '?code='
    url += str(station_code)
    url += str(QUANTITY_CODES[station_type])
    url += '&annee='
    url += str(year)
    url += '&mois='
    url += str(month)
    url += '&xt=prt'
    return url

def retrieveStatHoraireTab(url):
    """
    Returns a bs4 object (soup) from the requested url.
    parameter: url (string)
    """
    soup = None
    # we ask the website for printable output (xt=prt) because it is cleaner and easier to parse
    # define a request object
    req = Request(url)
    try:
        # open the http connection with the server
        urlopen(req)
    # catch a few observed exceptions
    except HTTPError as e:
        print('skipping {url} cuz http Error code: ', e.code)
    except URLError as e:
        print('skipping {url} cuz url Reason: ', e.reason)
    else:
        # read the raw html from the connection (sauce in BeautifulSoup parlance)
        sauce = urlopen(url).read()
        # parse the sauce into soup
        soup = bs.BeautifulSoup(sauce, 'lxml')
    return soup

def parseMeasurements(soup):
    """
    Parses the passed soup and returns a 2D Numpy array
    maintaining the measurements in the original orientation:
        d01  d02  d03  ...  d31
    h01
    h02
    ...
    h24

    Not all cells are filled for all months":
    The current month fills up as time progresses.
    All tables have 31 columns: so not all of them can be meaningfully filled.
    The numpy array has no labels for rows or columns.
    """
    all_tables = soup.find_all(name='table')

    
    # measurements_table = all_tables.find('table', attrs={'cellspacing':'2', 'cellpadding':'2',  'border':'0', 'width':'100%'})
    measurements_table = all_tables[4]
    # print(measurements_table)

    # we do not check if this table actually is there at all

    # # find the header row containing the days of the month
    # header_row = measurements_table.find('tr', attrs={'align':'center'})
    # header_cells = header_row.find_all('th', attrs={'class':'statmois'})
    # days = [cell.text for cell in header_cells]
    # print(days)

    # as it turns out: the table always has 31 columns, not all of them are filled with a measurement

    measurements_rows = measurements_table.find_all('tr', attrs={'align':'right'})
    # we can probably assume there will always be 24 hours reported in the table
    # but we wil not grow the table any wider than necessary
    empty_column = np.zeros((24, 1))
    empty_column[empty_column==0] = UNKNOWN_FLOAT
    # print(empty_column)
    X = empty_column

    # iterate over the rows of the table (hours of the day)
    for hour in range(1, 25):
        row = measurements_rows[hour]
        # attributes for cells differ with status of data (verified or not, for instance)
        # so we take all cells and skip the first one containing hour label (row header)
        # then we check contents of the cell and fill/grow the array as appropriate
        measurement_cells = row.find_all('td')
        for day in range(1, len(measurement_cells)):
            measurement = measurement_cells[day]
            if measurement.text:
                if day - 1 > X.shape[1] - 1:
                    X = np.append(X, empty_column, axis=1)
                X[hour - 1, day - 1] = float(measurement.text)
    #
    return X

def create_table_measurement():
    """
    Creates the TimescaleDB table for measurements.
    """
    conn = psycopg2.connect(CONNECTION_DETAILS)
    cursor = conn.cursor()
    print("connected to database meuse")

    # q = """
    #     DROP TABLE IF EXISTS wallonie.measurement;
    #     """
    # cursor.execute(q)
    # conn.commit()
    # print('table wallonie.measurement cleared')

    columns = [
        "datetime timestamp without time zone NOT NULL",
        "station_code integer NOT NULL",
        "quantity character(32) NOT NULL",
        "value numeric NOT NULL",
        "aggr_period integer NOT NULL",
    ]

    table_name = f"wallonie.measurement"
    q = f"CREATE TABLE IF NOT EXISTS {table_name}\n"
    q += "(\n"
    q += ",\n".join(columns)
    q += ",\n"
    q += "PRIMARY KEY(datetime, station_code, quantity)\n"
    q += ")\n"
    q += ";"

    print(q)
    cursor.execute(q)
    conn.commit()
    print(f"table {table_name} created")
    cursor.close()
    conn.close()
    print("connection closed")
    #

def insert_records_measurement(X, station_code, station_type, year, month):
    """
    Takes a numpy array with a month worth of measurements (hours x days),
    stores them in a chronological Postgres Database.
    Parameters:
        X: np array with measurements
        station_code: station by code (string or integer)
        station_type: type of the station (string, key into QUANTITY_CODES)
        year: (string or integer)
        month: (string or integer)
    """
    [num_hours, num_days] = X.shape
    # print(pd.DataFrame(X))
    table_name = "wallonie.measurement"

    conn = psycopg2.connect(CONNECTION_DETAILS)
    cursor = conn.cursor()
    print("connected to database meuse")

    print("start inserting/updating measurements")
    # into = ['datetime', 'station_code', 'quantity', 'value', 'aggr_period']
    # values = ["%(datetime)s", "%(station_code)s", "%(quantity)s", "%(value)s", "%(aggr_period)s"]
    # update = []
    # for column_name in into:
    #     update.append(f"{column_name} = EXCLUDED.{column_name}")

    # q = ""
    # q += f"INSERT INTO {table_name}\n"
    # q += f"({', '.join(into)})\n"
    # q += f"VALUES ({', '.join(values)})\n"
    # q += f"ON CONFLICT (code) DO\n"
    # q += f"    UPDATE SET\n"
    # q += f"        {', '.join(update)}\n"
    # q += ";"


    q = """
        INSERT INTO wallonie.measurement
        (datetime, station_code, quantity, value, aggr_period)
        VALUES (%(datetime)s, %(station_code)s, %(quantity)s, %(value)s, %(aggr_period)s)
        ON CONFLICT (datetime, station_code, quantity) DO
            UPDATE SET
                datetime = EXCLUDED.datetime, station_code = EXCLUDED.station_code, quantity = EXCLUDED.quantity, value = EXCLUDED.value, aggr_period = EXCLUDED.aggr_period
        ;
    """


    print(q)

    for day in range(1, num_days + 1):
        for hour in range(1, num_hours + 1):
            # we cannot find any information on the website about timing and timezone of the reported measurements
            # so, let's just assume/guess that all times mentioned are in localtime without DST
            # and also that each measurement reports the sum or mean aggregated over the past hour.
            # Although, it might be that the measurement aggregates the 60 minute period AROUND the (top of the) hour...
            datetime_string = "{:04d}-{:02d}-{:02d} {:02d}:00:00+01".format(year, month, day, hour)
            value = X[hour - 1, day - 1]
            if value != UNKNOWN_FLOAT:
                v = {
                    'datetime': datetime_string,
                    'station_code': station_code,
                    'quantity': station_type,
                    'value': value,
                    'aggr_period': 3600,
                }
                print(v)
                cursor.execute(q, v)
    conn.commit()
    print(f"table {table_name} created")
    cursor.close()
    conn.close()
    print("connection closed")

    #
    return True


station_test = 1579
type_test = 'precipitation'
year_test = 2019
month_test = 6

# create_table_measurement()

# print(station_test)
url = build_url_StatHoraireTab(station_test, type_test, year_test, month_test)
print(url)
soup = retrieveStatHoraireTab(url)
# print(soup)
m = parseMeasurements(soup)

insert_records_measurement(m, station_test, type_test, year_test, month_test)

