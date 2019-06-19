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

# df to store status of data coverage in our timescalDB
# the columns 'coverage' could have the following values (if only we had them all implemented in code):
#  covered: we scraped the data and inserted it in the DB [implemented]
#  bare: we need to scrape this [implemented]
#  incomplete: this year-month is not (yet) complete; likely the current month [implemented]
#  annotated: some or all cells in the table are annotated by the website [todo]
#  nonvalidated: some or all cells in the table are not (yet) validated by the website [todo]
#  unknown: default status until we know better [implemented]

DATA_COVERAGE_FILENAME = 'data_coverage.csv'
if os.path.exists(DATA_COVERAGE_FILENAME) and os.path.isfile(DATA_COVERAGE_FILENAME):
    data_coverage = pd.read_csv(DATA_COVERAGE_FILENAME)
else:
    data_coverage = pd.DataFrame(columns = ['station_type', 'station_code', 'year', 'month', 'coverage'])
    data_coverage.to_csv(DATA_COVERAGE_FILENAME, index=False, header=True, columns=['station_type', 'station_code', 'year', 'month', 'coverage'])

# In order to decide if we are dealing with a year-month still going on (implying
# that the table cannot be complete yet), we need to know what year/month it is now.
# The website uses timezone UTC+01, as far as I can see. No DST.
# We are comparing UTC (time.time()) without Zone of DST.
# But this script might be running for hours on end. We determine "now" only once, at the start of it.
# For these (and some other) reasons, we apply a margin. We test if the requested month
# could be near "now" by comparing it with a recent moment and a soon moment.

TIME_MARGIN = 3600 * 25 # a 25 hour margin: do not make this larger than half a month
(recent_year, recent_month) = time.localtime(time.time() - TIME_MARGIN)[0:2]
(soon_year, soon_month) = time.localtime(time.time() + TIME_MARGIN)[0:2]

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

    
    # measurements_table = all_tables.find('table', attrs={'cellspacing':'2', 'cellpadding':'2',  'border':'0', 'width':'100%'})

    # Issue #3 https://github.com/riklmr/chaudfontaine/issues/3
    measurements_table = all_tables[-2]
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
    # empty_column = np.zeros((24, 1))
    # empty_column[empty_column==0] = UNKNOWN_FLOAT
    # print(empty_column)
    # X = empty_column

    X = {}

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
                # BUG: https://github.com/riklmr/chaudfontaine/issues/1
                # if day - 1 > X.shape[1] - 1:
                #     X = np.append(X, empty_column, axis=1)
                # X[hour - 1, day - 1] = float(measurement.text)
                datetime_string = "{:04d}-{:02d}-{:02d} {:02d}:00:00+01".format(year_www, month_www, day, hour)
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

def insert_records_measurement(X, station_code, station_type, year, month):
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

    start_year = max(int(start_year), earliest_year)
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
    print(f"found {len(stations_meuse_db)} {station_type} stations in db in watershed Meuse")
    return list(stations_meuse_db.index)

def etl_station_month(station_code, station_type, year, month):
    """
    Performs ETL for one station (of one type) for one year-month.
    Parameters: station_code, station_type, year, month.
    """
    coverage = 'unknown'

    url = build_url_StatHoraireTab(station_code, station_type, year, month)
    print(station_code, station_type, year, month, url)
    soup = retrieveStatHoraireTab(url)
    time.sleep(SLEEPTIME) # courtesy to the webserver

    if soup:
        if access_authorized(soup):
            measurements_dict = parseMeasurements(soup)
            insert_records_measurement(measurements_dict, station_code, station_type, year, month)
            coverage = 'covered'
        else:
            print(station_code, "access not authorized", url)
            coverage = 'unavailable'
    else:
        print("no measurements for", station_code, station_type, year, month, file=sys.stderr)
        coverage = 'unknown'
    #
    return coverage

def process_station_month(station_code, station_type, year, month, cover=['bare', 'unknown']):
    """
    Processes ETL for one station (of one type) for one year-month.
    Keeps track of data coverage. Skips when data coverage is not in the list of user
    requested coverage states.
    Parameters: station_code, station_type, year, month.
        cover: list of coverage states that the user wants 'covered', defaults to ['bare', 'unknown'].
    """

    station_type_year_month = (
        (data_coverage['station_code'] == station_code) &
        (data_coverage['station_type'] == station_type) &
        (data_coverage['year'] == year) &
        (data_coverage['month'] == month)
    )
    coverage_df = data_coverage.loc[station_type_year_month, ['coverage']]
    if coverage_df.empty:
        coverage = 'unknown'
    else:
        coverage = coverage_df.iloc[0,0]
    #
    if coverage in cover:
        coverage = etl_station_month(station_code, station_type, year, month)
        if (coverage == 'covered') and ( (year, month)==(recent_year, recent_month) or (year, month)==(soon_year, soon_month) ):
            # we are (probably?) parsing the current month, so let's flag it as incomplete for now
            coverage = 'incomplete'
        #
    else:
        print( "skipping as not requested", station_code, station_type, year, month, coverage)
    #
    data_coverage.loc[station_type_year_month, ['coverage']] = coverage

def etl_meuse_month(station_type, year, month):
    """
    Performs ETL for all stations (of one type) in the watershed Meuse for one year-month.
    Parameters: station_type, year, month.
    """
    for station_code in all_stations_meuse(station_type):
        process_station_month(station_code, station_type, year, month)

def etl_meuse_alltime(station_type, earliest_year=2008):
    """
    Performs ETL for all stations (of one type) in the watershed Meuse 
    for all available year-months (of each station).
    Parameter: station_type.

    WARNING: this is the heaviest scraper of them all. Use wisely!
    """
    for station_code in all_stations_meuse(station_type):
        etl_station_alltime(station_code, station_type)
        data_coverage.to_csv(DATA_COVERAGE_FILENAME, index=False, header=True, columns=['station_type', 'station_code', 'year', 'month', 'coverage'])
    #

def etl_station_alltime(station_code, station_type, earliest_year=1990):
    """
    Performs ETL on one station, for all available year/months.
    Parameters: station_code (int or str), station_type (str).
    """
    url = build_url_StatHoraireTab(station_code, station_type)
    soup = retrieveStatHoraireTab(url)
    if soup:
        [start_date, end_date] = parsePeriod(soup)
        calendar = makeCalendar(start_date, end_date, earliest_year=earliest_year)
        for (year, month) in calendar:
            process_station_month(station_code, station_type, year, month)
    else:
        print("no soup found, no calendar created for", station_code, station_type, file=sys.stderr)
    #

def recover_crashed_run():
    # helper code to recover data status from last crashed run
    # we know where it crashed:
    crashed_type = 'precipitation'
    crashed_station = 9596

    # set up empty list of dicts
    daco = []
    # as we follow the footsteps of the crashed run, we can assume all station/type/year/months are covered
    # up until we encounter the crashed station/type
    data_status = 'covered'
    for station_type in QUANTITY_CODES.keys():
        for station_code in all_stations_meuse(station_type):
            if station_type == crashed_type and station_code == crashed_station:
                data_status = 'bare'
            #
            url = build_url_StatHoraireTab(station_code, station_type)
            soup = retrieveStatHoraireTab(url)
            [start_date, end_date] = parsePeriod(soup)
            calendar = makeCalendar(start_date, end_date, earliest_year=2000)
            for (year, month) in calendar:        
                daco.append( {
                    'station_code': station_code,
                    'station_type': station_type,
                    'year': year,
                    'month': month,
                    'coverage': data_status
                    } )
            #
    data_coverage = pd.DataFrame(data = daco)
    data_coverage.to_csv(DATA_COVERAGE_FILENAME, index=False, header=True, columns=['station_type', 'station_code', 'year', 'month', 'coverage'])

# example combos:
# hauteur: 2536
# debit: 6526
# precipitation: 5649

station_test = 5572
type_test = 'debit'
year_test = 2019
month_test = 6

# process_station_month(station_test, type_test, year_test, month_test, cover=['bare', 'unknown', 'incomplete'])

# etl_station_alltime(station_test, type_test, earliest_year=2000)

for station_type in QUANTITY_CODES.keys():
    etl_meuse_alltime(station_type, earliest_year=2000)
 
data_coverage.to_csv(DATA_COVERAGE_FILENAME, index=False, header=True, columns=['station_type', 'station_code', 'year', 'month', 'coverage'])

