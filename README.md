# Adobe Analytics Data Feeds and Google BigQuery

The package is intented to help in downloading Data Feeds from an FTP location, uploading into Google Cloud Storage and subsequently loading the files into BigQuery.

The process can run in either a Google-backed VM or in your local machine. Initially you need to setup your Data Feed extract (more info how to do so at my ["Analysing Adobe Analytics Data Feeds in Google BigQuery - Part 1"](https://www.linkedin.com/pulse/analysing-adobe-analytics-data-feeds-google-bigquery-papadopoulos/ "Analysing Adobe Analytics Data Feeds in Google BigQuery - Part 1")

### How to run the job manually:
1) Install all the requirements.txt file
2) Create a service account with the following access:
    1) BigQuery Data Editor
    2) BigQuery Job User
    3) Storage Object Viewer
3) Create a dataset to store the tables.
4) Create a bucket to store the files.
5) Update the main.py with the configuration settings for your environment.
6) Execute the main.py.
