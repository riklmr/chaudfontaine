import pandas as pd
import numpy as np
import time
import bs4 as bs
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import re
import json
import psycopg2

# the following strings represent Walloon rivers in the Meuse Watershed
MEUSE_WATERSHED = [
    'AMBLEVE',
    'BOCQ',
    'BROUFFE',
    'CHIERS',
    'EAU BLANCHE',
    "EAU D'HEURE",
    'EAU NOIRE',
    'FLAVION',
    'GEER',
    'GUEULE',
    'HANTES',
    'HERMETON',
    'HOEGNE',
    'HOLZWARCHE',
    'HOUILLE',
    'HOYOUX',
    'LESSE',
    'LHOMME',
    'LIENNE',
    'MEHAIGNE',
    'MEUSE',
    'MOLIGNEE',
    'OURTHE',
    'PIETON',
    "RY D'ERPION",
    "RY D'YVES",
    "RY DE ROME",
    'RY DE SOUMOY',
    'RY ERMITAGE',
    'RY FONT AUX SERPENTS',
    'RY JAUNE',
    'RY PERNELLE',
    'SAMBRE',
    'SEMOIS',
    'THURE',
    'VESDRE',
    'VIERRE',
    'VIROIN',
    'WARCHE',
    'WARCHENNE'
]

# The website *Les voies hydrauliques* encodes station types with these strings

## What is code 9002? Perhapsa debit that is not in the archive?
# example Vise: http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Actuelle/crue/mesure.jsp?code=54519002

QUANTITY_CODES = {
    'precipitation': '0015',
    'debit': '1002',
    'hauteur': '1011',
}

SLEEPTIME = 0.5 # seconds

CONNECTION_DETAILS_MEASUREMENT = "dbname='meuse' user='postgres' password='password' host='localhost' port='5333'"
CONNECTION_DETAILS_STATION = "dbname='meuse' user='postgres' password='password' host='localhost' port='5222'"

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


    conn = psycopg2.connect(CONNECTION_DETAILS_STATION)
    cursor = conn.cursor()
    # print("connected to database meuse")

    # print("start selecting stations")
    q = f"""
        SELECT {columns} FROM {table_name}
        ORDER BY {fields[0]} ASC
        LIMIT 10000;
        """
    cursor.execute(q)

    conn.commit()

    stations_list = cursor.fetchall()
    print(f"{len(stations_list)} stations(s) retrieved")
    stations_df = pd.DataFrame(columns=fields, data=stations_list)
    stations_df.set_index('code', inplace=True)

    # df = pd.concat([df, cursor.fetchall()])

    cursor.close()
    conn.close()
    # print("connection closed")
    return stations_df

def build_url_StatHoraireTab(station_code, station_type, year=None, month=None):
    """
    Returns the URL for the correct page, given:
    station by code (string or integer)
    type of the station (string, key into QUANTITY_CODES)
    year (string or integer or None for current year)
    month (string or integer or None for current year)
    """
    url = 'http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do'
    url += '?code='
    url += str(station_code)
    url += str(QUANTITY_CODES[station_type])
    if(year):
        url += '&annee='
        url += str(year)
    if(month):
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

def parsePeriod(soup):
    """
    Parses the passed soup and returns the period of available data for this station 
    as a tuple of two dates (ISO strings).
    """
    all_tables = soup.find_all(name='table')

    # periodic_table = all_tables.find('table', attrs={'cellspacing':'2', 'cellpadding':'2',  'border':'0', 'width':'100%'})
    periodic_table = all_tables[2]
    # this table has just one row
    periodic_row = periodic_table.find('tr')
    # with four cells
    periodic_cell = periodic_row.find_all('td')[2]

    # <td nowrap="" width="25%">Période : 01/2002 - 06/2019</td>
    [[start_month, start_year], [end_month, end_year]] = re.findall(r"(\d\d)\/(\d\d\d\d)", periodic_cell.text)
    start_date = "{:4}/{:2}/01 00:00:00+01".format(start_year, start_month)
    end_date = "{:4}/{:2}/01 00:00:00+01".format(end_year, end_month)

    return((start_date, end_date))

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
        row = measurements_rows[hour - 1]
        # attributes for cells differ with status of data (verified or not, for instance)
        # so we take all cells and skip the first one containing hour label (row header)
        # then we check contents of the cell and fill/grow the array as appropriate
        measurement_cells = row.find_all('td')
        for day in range(1, len(measurement_cells)):
            measurement = measurement_cells[day]
            if measurement.text:
                # BUG: https://github.com/riklmr/chaudfontaine/issues/1
                if day - 1 > X.shape[1] - 1:
                    X = np.append(X, empty_column, axis=1)
                X[hour - 1, day - 1] = float(measurement.text)
    #
    return X

def create_table_measurement():
    """
    Creates the TimescaleDB table for measurements.
    """
    conn = psycopg2.connect(CONNECTION_DETAILS_MEASUREMENT)
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
    # table_name = "wallonie.measurement"

    conn = psycopg2.connect(CONNECTION_DETAILS_MEASUREMENT)
    cursor = conn.cursor()
    # print("connected to database meuse")

    # print("start inserting/updating measurements")
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

    row_counter = 0

    for day in range(1, num_days + 1):
        for hour in range(1, num_hours + 1):
            # we cannot find any information on the website about timing and timezone of the reported measurements
            # so, let's just assume/guess that all times mentioned are in localtime (UTC+01) without DST
            # and also that each measurement reports the sum or mean aggregated over the past hour.
            # Although, I am pretty sure that the measurement aggregates the 60 minute period AROUND the (top of the) hour...
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
                cursor.execute(q, v)
                row_counter += 1
    conn.commit()
    print(f"{row_counter} row(s) inserted")

    cursor.close()
    conn.close()
    # print("connection closed")

    #
    return True

def makeCalendar(start_date, end_date):
    """
    Returns a list of tuples (year as int, month as int) containing 
    all year/month combinations between start_date and end_date, including.
    Parameters: start_date, end_date as ISO strings.
    """
    [[start_year, start_month]] = re.findall(r"^(\d\d\d\d)\/(\d\d)\/", start_date)
    [[end_year, end_month]] = re.findall(r"^(\d\d\d\d)\/(\d\d)\/", end_date)

    calendar = []

    # start_year may not be complete, so start at start_month
    for month in range(int(start_month), 13):
        calendar.append( (int(start_year), month) )
    
    # intervening years are complete, so start with 1, end with 12
    for year in range(int(start_year) + 1, int(end_year)):
        for month in range(1, 13):
            calendar.append( (year, month) )

    # end_year may not be complete (most likely this is the current year)
    for month in range(1, int(end_month) + 1):
        calendar.append( (int(end_year), month) )
    
    return calendar

def all_stations_meuse(station_type):
    """
    Returns a list of station_code of stations in the watershed of the Meuse.
    Parameter: station_type (string).
    """
    stations_db = get_stations_db(station_type)
    stations_meuse_db = stations_db[stations_db['river'].isin(MEUSE_WATERSHED)]
    print(f"found {len(stations_meuse_db)} stations in db in watershed Meuse")
    return list(stations_meuse_db.index)

def etl_station_month(station_code, station_type, year, month):
    """
    Performs ETL for one station (of one type) for one year-month.
    Parameters: station_code, station_type, year, month.
    """
    url = build_url_StatHoraireTab(station_code, station_type, year, month)
    print(station_code, station_type, year, month, url)
    soup = retrieveStatHoraireTab(url)
    measurements_df = parseMeasurements(soup)
    insert_records_measurement(measurements_df, station_code, station_type, year, month)

def etl_meuse_month(station_type, year, month):
    """
    Performs ETL for all stations (of one type) in the watershed Meuse for one year-month.
    Parameters: station_type, year, month.
    """
    for station_code in all_stations_meuse(station_type):
        # print(time.time())
        etl_station_month(station_code, station_type, year, month)
        time.sleep(SLEEPTIME)

def etl_meuse_alltime(station_type):
    """
    Performs ETL for all stations (of one type) in the watershed Meuse 
    for all available year-months (of each station).
    Parameter: station_type.

    WARNING: this is the heaviest scraper of them all. Use wisely!
    """
    

    for station_code in all_stations_meuse(station_type):
        etl_station_alltime(station_code, station_type)

def etl_station_alltime(station_code, station_type):
    """
    Performs ETL on one station, for all available year/months.
    Parameters: station_code (int or str), station_type (str).
    """
    url = build_url_StatHoraireTab(station_code, station_type)
    soup = retrieveStatHoraireTab(url)
    [start_date, end_date] = parsePeriod(soup)
    calendar = makeCalendar(start_date, end_date)
    for (year, month) in calendar:
        etl_station_month(station_code, station_type, year, month)
        time.sleep(SLEEPTIME)
    #

station_test = 5284 
type_test = 'precipitation'
year_test = 2009
month_test = 1

etl_station_month(station_test, type_test, year_test, month_test)
# etl_station_alltime(station_test, type_test)
# etl_meuse_month(type_test, year_test, month_test)
# etl_meuse_alltime(type_test)
