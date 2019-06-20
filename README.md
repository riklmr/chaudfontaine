# chaudfontaine
Harvest river measurements from stations in Wallonia. This repository was named after the measuring station [Chaudfontaine Piscine](https://www.openstreetmap.org/#map=16/50.5894/5.6537) in Wallonia of course. It measures La Vesdre just before it flows into L'Ourthe and into La Meuse.

The software has a [Github repo here](https://github.com/riklmr/chaudfontaine). The encompassing project is tagged #Meuse on [Github.com](https://github.com/search?q=%23meuse) and on [medium.com](https://medium.com/search/tags?q=%23Meuse).

You might find the additional information in [belleheid README.md](https://github.com/riklmr/belleheid) useful.

This Python 3 script:
- E - scrapes info from voies-hydrauliques.wallonie.be
- T - transforms html table into a dict
- L - stores the data chronologically in a PostgreSQL (TimescaleDB)

Read outline.md for details on (the state of) the implementation.

## Purpose
This script retrieves and stores data that will be used for training neural networks that model the flow of the river Meuse.

## How to retrieve archived measurements from voies-hydrauliques.wallonie.be
### Via e-mail
The website offers "direct downloads" of the tables in zipped CSV format. This requires you to leave your contact info (with e-mail address) in a webform. The webserver then proceeds to send you the requested file via one e-mail per table. Each table holds one station for the entire period of available data. This is a very short extract:
```csv
52911002; KELMIS (LA CALAMINE); GUEULE              ; débits horaires; 
date; heure1; heure2; heure3; heure4; heure5; heure6; heure7; heure8; heure9; heure10; heure11; heure12; heure13; heure14; heure15; heure16; heure17; heure18; heure19; heure20; heure21; heure22; heure23; heure24; 
2009-01-01; 0.654; 0.642; 0.642; 0.642; 0.642; 0.642; 0.642; 0.642; 0.629; 0.629; 0.629; 0.629; 0.629; 0.629; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 
2009-01-02; 0.617; 0.606; 0.606; 0.606; 0.606; 0.617; 0.617; 0.606; 0.606; 0.606; 0.606; 0.606; 0.617; 0.606; 0.606; 0.606; 0.606; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 0.617; 
``` 
The CSV reaches to the last available month (including).

It is doable to automate the request (see the prototypes). But the zipped CSV attachment needs to be extracted manually. Automating an e-mail scraper was too daunting for me. This method was abandoned.



### Via website tables
This url offers direct downloads of html tables:
http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do?code=62281002&mois=1&annee=2000&xt=prt

Tables are available month by month, station by station. Clicking through them is a lot of work, but this can be automated. This is how chaudfontaine now works.


## Python libraries needed
The usual: pandas, time, json, urllib, re, pickle.

Also: Beautiful Soup (bs4), Postgres client (psycopg2).

## Usage
This script has no commandline parameters. Edit the constants or function calls to suit your needs.

Most output is to STDOUT. Code is slowly changing error to print to STDERR.

This code assumes you set up PostgreSQL to receive your data. It also assumes you have another PostgreSQL DB holding tables of station meta data (See repo belleheid). Adjust the constants CONNECTION_DETAILS_* to your needs. The script has a function to create a suitable table.

Retrieving 240 year-months for one station takes about 7 minutes (using SLEEPTIME = 0.4).
This would probably improve a lot if we bunched up all records to insert into the DB and perform one single SQL command. 
However, we do not want to overload the scraped webserver, so slowly we plough through the data field.

### full downloads versus updates
The whole dataset is huge. It takes hours to download every single page (every type of measurement * every relevant station * every available year * 12 months). So we keep track of data coverage. That is: we answer the question "is this page in our DB already?".

The call to process_station_month() needs to be parameterized to process only pages with a given coverage status. This defaults to `want_covered = ['bare', 'unknown']`. This means that the script will process pages that are known to be not covered yet (bare) and pages that are unknown (this could mean anything, except we did not keep track).

In order to skip those pages, remove these coverage types fom the list and replace with different types. The parameter `want_covered = ['incomplete']` indicates you are only interested in updating pages the script has seen before, but were tracked as incomplete. This applies (in most cases) to pages of the current month. Those are still filling up hour by hour. Older pages might also have data holes in them, but (as of the current version) chaudfontaine ignores those holes. In the DBthose records will simply be missing. There are no NaN values in the DB.

### Being nice to the webserver
The constant SLEEPTIME adjusts a waiting period in between pages. This is my wayof being nice to the nice people operating the webserver. Courtesy from one server admin to another.

### Being greedy
The call to process_station_month() has the parameter earliest_year. Set this to a recent year to be less greedy. You script will run quickly through all the stations that way. Change it to earlier years as you need older/more data.

## Known issues
Clear!
