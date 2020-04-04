# Data lake with spark em S3

This project is an example of how to do transform data with spark and save this at S3.

## Architecture

The strategy used was to overwrite the fact and dimension table every time. This avoids duplicate values in the data lake.

### Project

* etl.py extract data from Udacity S3 bucket and transform to create fact and dimensions table.


### Run

First, configure your credentials at .cfg file and s3 bucket to save the new data.
Finally, run etl.py after finish confirm if you have your data into the s3 buckets.

