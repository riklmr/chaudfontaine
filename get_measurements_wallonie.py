"""
get_measurements_wallonie.py

Uses chaudfontaine to actually retrieve measurement data and store it/them
in the PostgreSQL database.

Usage: change earliest_year to a recent year and start filling your DB.
Change earliest_year to earlier years as you need more/older data.
"""

import chaudfontaine

# example combos:
# hauteur: 2536
# debit: 6526
# precipitation: 5649

# test parameters
station_type = 'debit'
station_code = 6526
year = 2019
month = 6


chaudfontaine.process_station_month(station_type, station_code, year, month, want_covered=['bare', 'unknown', 'incomplete'])

chaudfontaine.process_station_alltime(station_type, station_code, earliest_year = 2000, want_covered=['bare', 'unknown'])

# for station_type in chaudfontaine.QUANTITY_CODES.keys():
#     chaudfontaine.process_meuse_alltime(
#         station_type=station_type, 
#         earliest_year=1980,
#         want_covered=['bare', 'unknown'],
#     )

# chaudfontaine.save_data_coverage(data_coverage)

