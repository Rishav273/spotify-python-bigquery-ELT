# spotify-python-bigquery-ELT-project
An ELT project where music metadata is collected via the Spotify API and loaded into bigquery tables.

Used python spotipy library to batch load music metadata into bigquery tables. These will later be used for analysis and building dynamic dashboards and reporting tasks.

### Future improvements:
* Partition the data in bigquery according to some field (eg: date). 
* Bigquery table clustering.
* Current pipeline uses pandas to load the data. Will possibly try using an open source ingestion tool like Apache beam in the future.
* Adding logging and monitoring capabilities.
