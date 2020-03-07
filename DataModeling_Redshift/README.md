# DW with Redshit

This project is an example of how to load data from S3 into Redshift.

## Architecture

The architecture used was ELT because RedShift doesn't support upsert operation. Then I create two different schema one for DW and another for the staging area.

### Project

I add 3 files: 
* sql_queries.py has create, drop, insert and copy operations.
* crerate_tables.py has create and drop operations.
* etl.py has copy and insert operations into staging and DW tables.


### Run

First, configure your credentials at .cfg file, and then run create_tables.py.
Finally, run etl.py after finish confirm if you have your data into the tables.
