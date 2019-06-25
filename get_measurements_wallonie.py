"""
get_measurements_wallonie.py

Uses chaudfontaine to actually retrieve measurement data and store it/them
in the PostgreSQL database.

Usage: change earliest_year to a recent year and start filling your DB.
Change earliest_year to earlier years as you need more/older data.
"""

import chaudfontaine
etl = chaudfontaine.Chaudfontaine()

# example combos:
# hauteur: 2536, 8221 (Gendron/Lesse, very old station)
# debit: 6526
# precipitation: 5649

# etl.process_station_month(
#     station_type='hauteur', 
#     station_code=2536, 
#     year=2019, 
#     month=6, 
#     want_covered=['bare', 'unknown', 'incomplete']
# )

# etl.process_station_alltime(
#     station_type='hauteur', 
#     station_code=8221, 
#     earliest_year = 1965, 
#     want_covered=['bare', 'unknown']
# )

for station_type in etl.QUANTITY_CODES.keys():
    etl.process_meuse_alltime(
        station_type=station_type, 
        earliest_year=1965,
        want_covered=['bare', 'unknown'],
    )

etl.data_coverage.save()

