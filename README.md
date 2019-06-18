# chaudfontaine
Harvest river measurements from stations in Wallonia. This repository was named after the measuring station [Chaudfontaine Piscine](https://www.openstreetmap.org/#map=16/50.5894/5.6537) in Wallonia of course. It measures La Vesdre just before it flows into L'Ourthe and into La Meuse.

The software has a [Github repo here](https://github.com/riklmr/chaudfontaine).

You might find the additional information in [belleheid README.md](https://github.com/riklmr/belleheid) useful.

This Python script:
- E - scrapes info from voies-hydrauliques.wallonie.be
- T - transforms html table into a Numpy array
- L - stores the data chronologically in a PostgreSQL TimescaleDB

Read outline.md for details on (the state of) the implementation.

## Purpose
This script retrieves and stores data tht will be used for training neural networks that model the flow of the river Meuse.

## How to retrieve archived measurements from voies-hydrauliques.wallonie.be
### Via e-mail
The website offers "direct downloads" of the tables in zipped CSV format. This requires you to leave your contact info (with e-mail address) in a webform. The webserver then proceeds to send you the requested file via one e-mail per table. Each table holds one station for the entire period of available data. This is a very short extract:
```csv
52911002; KELMIS (LA CALAMINE); GUEULE              ; débits horaires; 
date; heure1; heure2; heure3; heure4; heure5; heure6; heure7; heure8; heure9; heure10; heure11; heure12; heure13; heure14; heure15; heure16; heure17; heure18; heure19; heure20; heure21; heure22; heure23; heure24; 
2009-01-01; 0.654; 0.642; 0.642; 0.642; 0.642; 0.642; 0.642; 0.642; 0.629; 0.629; 0.629; 0.629; 0.629; 0.629; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 
2009-01-02; 0.617; 0.606; 0.606; 0.606; 0.606; 0.617; 0.617; 0.606; 0.606; 0.606; 0.606; 0.606; 0.617; 0.606; 0.606; 0.606; 0.606; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 
``` 
The CSV reaches to the last available motnh (including).

It is doable to automate the request (see the prototypes). But the zipped CSV attachment needs to be extracted manually. Automating an e-mail scraper was too daunting for me. This method was abandoned.



### Via website tables
This url offers direct downloads of html tables:
http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do?code=62281002&mois=1&annee=2000&xt=prt

Tables are available month by month, station by station. Clicking through them is a lot of work, but this can be automated. This is how chaudfontaine now works.


## Python libraries needed
The usual: pandas, time, json, urllib, re.

Also: Beautiful Soup (bs4), Postgres client (psycopg2).

