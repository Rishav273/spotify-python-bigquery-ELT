# spotify-python-bigquery-ELT-project
#### An ELT project where music metadata is collected via the Spotify API and loaded into bigquery tables, visualized via Data Studio.

The Extract and Loading tasks were done using "extract_and_load_data.py" file, using bigquery client library and pandas_gbq package. Data was extracted using python's spotipy API client and loaded to a partitioned bigquery table. Logging procedures have also been applied in order to monitor every step of the pipeline.

All Transformation steps were done using SQL in BigQuery. The resultant data was stored in views and used to make dynamic dashboards via Google Data Studio.

Some screenshots have been attached below:

![image](https://user-images.githubusercontent.com/65105994/187060283-cb13bf29-99e5-4fc9-8894-9106c9c589e2.png)

![image](https://user-images.githubusercontent.com/65105994/187060314-0bee00d3-bb41-43a1-9a65-a69cc0e5cca6.png)

Future improvements:
* Collecting more metadata on genres of the tracks. Currently the spotify API doesn't expose any endpoint relating to track genre.
* Trying out big-data resilient tools like Apache Beam as an alternative to pandas dataframes, for ingesting higer volumes of data per batch.   
