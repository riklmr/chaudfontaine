import pandas as pd
import numpy as np
import time
import bs4 as bs
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError
import re
import json
import psycopg2
import sys, os
import pickle

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

## What is code 9002? Perhaps a debit that is not in the archive?
# example Vise: http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Actuelle/crue/mesure.jsp?code=54519002

QUANTITY_CODES = {
    'precipitation': '0015',
    'debit': '1002',
    'hauteur': '1011',
}

SLEEPTIME = 0.4 # seconds

CONNECTION_DETAILS_MEASUREMENT = "dbname='meuse' user='postgres' password='password' host='localhost' port='5333'"
CONNECTION_DETAILS_STATION = "dbname='meuse' user='postgres' password='password' host='localhost' port='5222'"

# pickled dict to store status of data coverage in our timescalDB
# the value could be any of thefollowing strings (if only we had them all implemented in code):
#  covered: we scraped the data and inserted it in the DB [implemented]
#  bare: we need to scrape this [implemented for unavailable pages]
#  incomplete: this year-month is not (yet) complete; likely the current month [implemented for current month only]
#  annotated: some or all cells in the table are annotated by the website [todo]
#  nonvalidated: some or all cells in the table are not (yet) validated by the website [todo]
#  unknown: default status until we know better [implemented]

DATA_COVERAGE_FILENAME = 'data_coverage.pickle'

# In order to decide if we are dealing with a year-month still going on (implying
# that the table cannot be complete yet), we need to know what year-month it is now.
# The website uses timezone UTC+01, as far as I can see. No DST.
# We are comparing UTC (time.time()) without Zone or DST.
# But this script might be running for hours on end. We determine "now" only once, at the start of it.
# For these (and some other) reasons, we apply a margin. We test if the requested month
# could be near "now" by comparing it with a recent moment and a soon moment.

TIME_MARGIN = 3600 * 25 # a few hours margin in seconds: do not make this larger than half a month
(recent_year, recent_month) = time.localtime(time.time() - TIME_MARGIN)[0:2]
(soon_year, soon_month) = time.localtime(time.time() + TIME_MARGIN)[0:2]


def save_data_coverage(data_coverage):
    pickled_dict = open(DATA_COVERAGE_FILENAME, mode='wb')
    pickle.dump(data_coverage, pickled_dict)
    pickled_dict.close()
    print(len(data_coverage), "tracked pages saved")

def init_data_coverage():
    print("init data coverage tracking")
    if os.path.exists(DATA_COVERAGE_FILENAME) and os.path.isfile(DATA_COVERAGE_FILENAME):
        pickled_dict = open(DATA_COVERAGE_FILENAME, mode='rb')
        data_coverage = pickle.load(pickled_dict)
        pickled_dict.close()
    else:
        data_coverage = {}
        save_data_coverage(data_coverage)
    print(len(data_coverage), "pages already tracked")
    return data_coverage

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

def build_url_StatHoraireTab(station_type, station_code, year=None, month=None):
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
    print(url)
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

def parseYearMonth(soup):
    """
    Parses soup and parses the year and month of the current table from the form.
    Returns: Tuple of integers: (year, month)
    """

    # <form action="stathorairetab.do">
    #   <input type="hidden" value="80630015" name="code">
    #   <input maxlength="2" size="2" type="text" name="mois" value="01">
    # 	<input maxlength="4" size="4" type="text" name="annee" value="2019">
    #   <input value=" " type="submit">
    # </form>
    form = soup.find('form', attrs = {'action': 'stathorairetab.do'})
    mois = form.find('input', attrs = {'name': 'mois'})
    annee = form.find('input', attrs = {'name':'annee'})

    if annee['value']:
        year = int(annee['value'])
    else:
        year = None
    if mois['value']:
        month = int(mois['value'])
    else:
        month = None
    return((year, month))

def access_authorized(soup):
    access = True
    # <h1>Accès non autorisé</h1>
    headings = soup.find_all('h1')
    for h1 in headings:
        if re.search(r'Accès non autorisé', h1.text):
            access = False
    return(access)

def parseMeasurements(soup):
    """
    Parses the passed soup and returns a DICT
    REPLACING the original orientation:
        d01  d02  d03  ...  d31
    h01
    h02
    ...
    h24
    WITH a chronological orientation.
    {
        datetimestring: value,
        ...,
        datetimestring: value,
    }
    Not all cells are filled for all months":
    The current month fills up as time progresses.
    All tables have 31 columns: so not all of them can be meaningfully filled.
    Some months have holes inthe data.
    """

    [year_www, month_www] = parseYearMonth(soup)

    all_tables = soup.find_all(name='table')

    # resolved Issue #3 https://github.com/riklmr/chaudfontaine/issues/3
    measurements_table = all_tables[-2]
    # we do not check if this table actually is there at all

    # setup the empty dict X
    X = {}

    measurements_rows = measurements_table.find_all('tr', attrs={'align':'right'})

    # iterate over the index of the rows in the table (hours of the day)
    # skipping the first row containing column headers
    for hour in range(1, 25):
        row = measurements_rows[hour - 1]
        # attributes for cells differ with status of data (verified or not, for instance)
        # so we take all cells and skip the first one containing hour label (row header)
        measurement_cells = row.find_all('td')
        # iterate over the index of the cells in the row (days of the month)
        for day in range(1, len(measurement_cells)):
            measurement = measurement_cells[day]
            if measurement.text:
                datetime_string = "{:04d}-{:02d}-{:02d} {:02d}:00:00+01".format(year_www, month_www, day, hour)

                # github issue #5
                if re.search(r"[\*]", measurement.text ):
                    # string contains non-numerical chars that we associate with special circumstances (like annotated values)
                    # not a problem, we accept these values but we need to remove the non-numerical chars
                    X[datetime_string] = float(measurement.text.replace('*', ''))
                else:
                    X[datetime_string] = float(measurement.text)
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

def insert_records_measurement(X, station_type, station_code, year, month):
    """
    Takes a dict with a month worth of measurements (created by parseMeasurements()),
    stores them in a chronological Postgres Database.
    Parameters:
        X: dict with measurements, key=datetimestring, value=float
        station_code: station by code (string or integer)
        station_type: type of the station (string, key into QUANTITY_CODES)
        year: (string or integer)
        month: (string or integer)
    """
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

    for datetime_string in X.keys():
        v = {
            'datetime': datetime_string,
            'station_code': station_code,
            'quantity': station_type,
            'value': X[datetime_string],
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

def makeCalendar(start_date, end_date, earliest_year=1950):
    """
    Returns a list of tuples (year as int, month as int) containing 
    all year/month combinations between start_date and end_date, including.
    Parameters: start_date, end_date as ISO strings.
    earliest_year (int): indicates the earliest year we want to scrape
        this prevents scraping of VERY DEEP archives
    """
    [[start_year, start_month]] = re.findall(r"^(\d\d\d\d)\/(\d\d)\/", start_date)
    [[end_year, end_month]] = re.findall(r"^(\d\d\d\d)\/(\d\d)\/", end_date)
    start_year = int(start_year)
    start_month = int(start_month)
    end_year = int(end_year)
    end_month = int(end_month)

    start_year = max(start_year, earliest_year)
    calendar = []

    # start_year may be the same as end_year, if so: skip it and skip intervening years
    # also skip when start_year mistakenly follows end_year
    if start_year < end_year:
        # start_year may not be complete, so start at start_month
        for month in range(start_month, 13):
            calendar.append( (start_year, month) )
        
        # intervening years are complete, so start with 1, end with 12
        for year in range(start_year + 1, end_year):
            for month in range(1, 13):
                calendar.append( (year, month) )

    # end_year may not be complete (most likely this is the current year)
    for month in range(1, end_month + 1):
        calendar.append( (end_year, month) )
    
    return calendar

def all_stations_meuse(station_type):
    """
    Returns a list of station_code of stations in the watershed of the Meuse.
    Parameter: station_type (string).
    """
    stations_db = get_stations_db(station_type)
    stations_meuse_db = stations_db[stations_db['river'].isin(MEUSE_WATERSHED)]
    print(f"found {len(stations_meuse_db)} {station_type} stations in db in watershed Meuse")
    return list(stations_meuse_db.index)

def etl_station_month(station_type, station_code, year, month):
    """
    Performs ETL for one station (of one type) for one year-month.
    Parameters: station_type, station_code, year, month.
    """
    coverage = 'unknown'

    url = build_url_StatHoraireTab(station_type, station_code, year, month)
    soup = retrieveStatHoraireTab(url)
    time.sleep(SLEEPTIME) # courtesy to the webserver

    if soup:
        if access_authorized(soup):
            measurements_dict = parseMeasurements(soup)
            insert_records_measurement(measurements_dict, station_type, station_code, year, month)
            coverage = 'covered'
        else:
            print(station_code, "access not authorized", url)
            coverage = 'bare'
    else:
        print("no measurements for", station_type, station_code, year, month, file=sys.stderr)
        coverage = 'unknown'
    #
    return coverage

def process_station_month(station_type, station_code, year, month, want_covered=['bare', 'unknown']):
    """
    Processes ETL for one station (of one type) for one year-month.
    Keeps track of data coverage. Skips when data coverage is not in the list of user
    requested coverage states.
    Parameters: station_type, station_code, year, month,
        want_covered: list of coverage states that the user wants to cover, 
        defaults to ['bare', 'unknown'].
    """
    # serialize four vars into a key for dict data_coverage
    coverage_key = "{}-{}-{}-{}".format(station_type, station_code, year, month)

    if coverage_key in data_coverage.keys():
        old_coverage = data_coverage[coverage_key]
    else:
        old_coverage = 'unknown'
    #
    if old_coverage in want_covered:
        print("scraping wanted page", coverage_key, old_coverage)
        new_coverage = etl_station_month(station_type, station_code, year, month)
        if (new_coverage == 'covered') and ( (year, month)==(recent_year, recent_month) or (year, month)==(soon_year, soon_month) ):
            # we are (probably?) parsing the current month, so let's flag it as incomplete for now
            new_coverage = 'incomplete'
        #
        data_coverage[coverage_key] = new_coverage
    else:
        print("skipping unwanted page", coverage_key, old_coverage)
        new_coverage = old_coverage
    #

    return new_coverage

def process_meuse_month(station_type, year, month):
    """
    Performs ETL for all stations (of one type) in the watershed Meuse for one year-month.
    Parameters: station_type, year, month.
    """
    for station_code in all_stations_meuse(station_type):
        process_station_month(station_type, station_code, year, month)

def process_meuse_alltime(station_type):
    """
    Performs ETL for all stations (of one type) in the watershed Meuse 
    for all available year-months (of each station).
    Parameter: station_type.

    WARNING: this is the heaviest scraper of them all. Use wisely!
    """
    for station_code in all_stations_meuse(station_type):
        process_station_alltime(station_type, station_code)
        save_data_coverage(data_coverage)
    #

def process_station_alltime(station_type, station_code, earliest_year=1990):
    """
    Performs ETL on one station, for all available year/months.
    Parameters: station_type (str), station_code (int or str).
    """
    url = build_url_StatHoraireTab(station_type, station_code)
    soup = retrieveStatHoraireTab(url)
    if soup:
        [start_date, end_date] = parsePeriod(soup)
        calendar = makeCalendar(start_date, end_date, earliest_year=earliest_year)
        for (year, month) in calendar:
            process_station_month(station_type, station_code, year, month)
    else:
        print("no soup found, no calendar created for", station_type, station_code, file=sys.stderr)
    #


# example combos:
# hauteur: 2536
# debit: 6526
# precipitation: 5649

# test parameters
station_type = 'debit'
station_code = 5572
year = 2017
month = 1

data_coverage = init_data_coverage()


process_station_month(station_type, station_code, year, month, want_covered=['bare', 'unknown', 'incomplete'])

# process_station_alltime(station_type, station_code, earliest_year = 2018)

# for station_type in QUANTITY_CODES.keys():
#     process_meuse_alltime(station_type)
 
save_data_coverage(data_coverage)


