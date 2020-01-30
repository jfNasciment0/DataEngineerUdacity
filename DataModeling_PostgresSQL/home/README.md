# Data Modeling with Postgres
At this project, I developed an ETL to process data from a startup called Sparkify. This company has a music player service. The objective was to create a simple, but robust ETL pipeline using Python and Postgres SQL.

## Getting Start

First, check if you have python3 installed, if not you can install using brew install python3.

Now run the create_table.py to setup our environment in Postgres SQL.
```
  python3 create_table.py
```
After that, we can check if our database and tables were created running this file test.ipynb. If everything is ok you can see the following database and tables.

Database:
  * sparkifydb
Tables:
  * songplays
  * users
  * songs
  * artists
  * time

Run etl.py to populate this tables, and our fact table. In the end in your fact table songplays you have only one value.
