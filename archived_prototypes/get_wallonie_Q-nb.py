#%% [markdown]
# # request data from sethy wallonie
# Region Wallonia, Belgium has interesting data: hourly measurements of Q, R and H for may stations along the Meuse.
# 

#%%
import time
import requests


#%%
# Q
debit_stations = {
    "5291":"KELMIS (LA CALAMINE)",
    "5572":"BERGILERS Amont",
    "5826":"SAUHEID",
    "5921":"TABREUX",
    "5953":"DURBUY",
    "5962":"HOTTON",
    "5991":"NISRAMONT",
    "6021":"MABOMPRE",
    "6122":"ORTHO",
    "6228":"CHAUDFONTAINE Pisc",
    "6526":"BELLEHEID",
    "6621":"MARTINRIVE",
    "6671":"TARGNON",
    "6732":"STAVELOT",
    "6753":"LASNENVILLE",
    "6803":"CHEVRON",
    "6832":"TROIS-PONTS",
    "6933":"MALMEDY",
    "6946":"BEVERCE",
    "6971":"WIRTZFELD",
    "6981":"BULLINGEN",
    "6991":"MALMEDY Warchenne",
    "7132":"AMAY",
    "7228":"MODAVE",
    "7242":"MOHA",
    "7244":"HUCCORGNE",
    "7319":"SALZINNES Ronet (Namur)",
    "7487":"SOLRE",
    "7711":"JAMIOULX",
    "7781":"WALCOURT (GARE)",
    "7784":"WALCOURT (SEUIL)",
    "7812":"WALCOURT-VOGENEE",
    "7831":"SILENRIEUX (BARRAGE,)",
    "7843":"BOUSSU-LEZ-WALCOURT",
    "7863":"SILENRIEUX (RY)",
    "7883":"SOUMOY",
    "7891":"CERFONTAINE",
    "7944":"WIHERIES",
    "7978":"BERSILLIES-L'ABBAYE",
    "8134":"YVOIR",
    "8163":"WARNANT",
    "8166":"SOSOYE",
    "8181":"FOY",
    "8221":"GENDRON",
    "8341":"DAVERDISSE",
    "8527":"JEMELLE",
    "8622":"HASTIERE",
    "8661":"FELENNE",
    "8702":"CHOOZ",
    "9021":"TREIGNES",
    "9071":"COUVIN",
    "9081":"NISMES",
    "9111":"MARIEMBOURG",
    "9201":"COUVIN Ry de Rome",
    "9221":"PETIGNY Ry de Rome",
    "9223":"PETIGNY Ermitage",
    "9224":"PETIGNY Fd Serpents",
    "9232":"BRULY",
    "9434":"MEMBRE Pont",
    "9461":"BOUILLON",
    "9541":"CHINY",
    "9561":"TINTIGNY",
    "9571":"SAINTE-MARIE",
    "9651":"STRAIMONT",
    "9741":"TORGNY",
}


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
#%% [markdown]
# ```
# Tableau
# Graphique
# Téléchargement
# ```

#%%



#%%


station_code = list(debit_stations.keys())[0]
obs_code = "1002"

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
url += "code="
url += station_code
url += obs_code


#%%
print(url)

response = requests.get(url)
#%%
for station_code in list(debit_stations.keys()):
    station_name = debit_stations[station_code]
    this_url = url + "code="
    this_url += station_code
    this_url += obs_code
    print("requesting hourly Q from", station_code, station_name, this_url)
    response = requests.get(this_url)
    print("sleeping 20 seconds...")
    time.sleep(20)
#


#%%
len(debit_stations)


#%%



#%%



#%%



#%%



