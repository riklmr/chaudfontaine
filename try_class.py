
import chaudfontaine

etl = chaudfontaine.Chaudfontaine(filename='tr_class_dc.pickle')

# url = etl.build_url_StatHoraireTab(station_type='hauteur', station_code=2536)
# print(url)

# etl.process_station_month(
#     station_type='hauteur', 
#     station_code=2536, 
#     year=2019, 
#     month=6, 
#     want_covered=['bare', 'unknown', 'incomplete']
# )


etl.process_station_alltime(
    station_type='hauteur', 
    station_code=2536, 
    earliest_year = 2018, 
    want_covered=['bare', 'unknown']
)

