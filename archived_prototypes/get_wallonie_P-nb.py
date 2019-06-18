#%% [markdown]
# # request precipitation data from sethy wallonie
# Region Wallonia, Belgium has interesting data: hourly measurements of Q, R and H for may stations along the Meuse.
# 

#%%
import time
import requests
import pandas as pd


#%%
get_ipython().system('ls datasets/wallonie/stations*csv')


#%%
precip_stations = pd.read_csv("datasets/wallonie/stations_wallonia_precipitation.csv", 
                              index_col='Code'
                             )


#%%
precip_stations = precip_stations.loc[precip_stations['Recipient'] != "0"]


#%%
precip_stations.drop(columns=['Precipitation'], inplace=True)


#%%
precip_stations.head()


#%%
precip_stations.describe()


#%%
for station_code in precip_stations.index:
    print(station_code, precip_stations.loc[station_code, 'Nom'])


#%%



#%%



#%%



#%%



#%%
# about the requester, make sure the eimail address is valid
# you will receive all zipped csv files here
name = "RikDeDeken"
address = "Buitendijk3HeinenoordNL"
email = "hydro%40parelmoer.xs4all.nl"

#%% [markdown]
# ```
# code  explanation   type of observation
# 0015  precipitation Pluviographes
# 1002  debit         Limnigraphes
# 1011  hauteur       Limnigraphes
# ```

#%%
# station_code = list(debit_stations.keys())[0]
obs_code = "0015"


#%%
url  = "http://voies-hydrauliques.wallonie.be"
url += "/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetele.do?"
url += "PD=Téléchargement"
url += "&"
url += "nom="
url += name
url += "&"
url += "adresse="
url += address
url += "&"
url += "email="
url += email
url += "&"
url += "objectif=etude"
url += "&"
url += "util=util"
url += "&"
# url += "code="
# url += station_code
# url += obs_code


#%%
url

response = requests.get(url)
#%%
for station_code in precip_stations.index:
    station_name = precip_stations.loc[station_code, 'Nom']
    this_url = url + "code="
    this_url += str(station_code)
    this_url += obs_code
    print("requesting hourly Precipitation from", station_code, station_name, this_url)
    response = requests.get(this_url)
    
    print("sleeping 23 seconds...")
    time.sleep(23)
#


#%%



#%%



#%%



#%%



