import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Date, TimestampType as Timestamp

def set_aws_access(file):
    """
        This function define the AWS access enviroment
        Param: 
            File
    """
    try:
        config = configparser.ConfigParser()
        config.read(file)
        os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
        os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
    except:
        print('Error set AWS environment !!')


def create_spark_session():
    # Define spark session
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

    
def set_data_schema(schema):
    """ 
        This Function return the schema definition
        Param:
            schema name
        Output:
            schema
    """
    print("...Setting data schema")
    try:
        songtSchema = R([    
            Fld("artist_id", Str()), 
            Fld("artist_latitude", Str()), 
            Fld("artist_longitude", Str()), 
            Fld("artist_location", Str()),          
            Fld("artist_name", Str()), 
            Fld("song_id", Str()), 
            Fld("title", Str()), 
            Fld("duration", Dbl()),
            Fld("year", Int())
        ]) 

        eventSchema = R([  
            Fld("artists", Str()),
            Fld("auth", Str()),
            Fld("first_name", Str()),
            Fld("gender", Str()),
            Fld("item_in_session", Int()),
            Fld("last_name", Str()),
            Fld("lenght", Str()),
            Fld("level", Str()),
            Fld("location", Str()),
            Fld("method", Str()),
            Fld("page", Str()),
            Fld("registration", Str()),
            Fld("session_id", Int()),
            Fld("song", Str()),
            Fld("status", Int()),
            Fld("ts", Str()),
            Fld("user_agente", Str()),
            Fld("user_id", Int())
        ])
        create_schema = {'Song': songtSchema, 'Event': eventSchema}
        return create_schema[schema]
    except:
        return "Error schema not exists"
    

def readJsonFiles(spark, input_data, data_schema):
    """
    
    """
    # read data file
    print("...Reading data from S3")
    print(input_data)
    #print(data_schema)
    try:
        data_df = spark.read.json(input_data, schema = data_schema)
        data_df.limit(2)
        return data_df
    except:
        #data_df = spark.read.json(input_data, schema = data_schema)
        #return data_df
        return "Error load data"


def process_song_data(spark, df_data, output_data):
    """
        This Function write song and artist table in parquet format.
        Param:
            spark: spark session
            df_data: Dataframe from json song
            output_data: path to save table on s3
    """

    # extract columns to create songs table
    song_fields = ["song_id", "title", "artist_id", "year", "duration"]
    songs_table = df_data.select(song_fields).dropDuplicates(['song_id'])

    # write songs table to parquet files partitioned by year and artist
    song_out_path = str(output_data + 'songs/songs.parquet')
    songs_table.write.parquet(song_out_path, mode = ' overwrite', partitionBy = ['year', 'artist_id'])

    # extract columns to create artists table
    artist_mapping = dict(zip(["artist_id", "artist_id"], 
                              ["artist_name", "name"], 
                              ["artist_location", "location"], 
                              ["artist_latitude", "latitude"], 
                              ["artist_longitude", "longitude"]
    ))
    artists_table = df_data.select([col(c).alias(artist_mapping.get(c, c)) for c in df_data.columns]).dropDuplicates(['artist_id'])
    
    # write artists table to parquet files
    artist_out_path = str(output_data + 'artists/artists.parquet')
    artists_table.write.parquet(artist_out_path, mode='overwrite') 


def process_log_data(spark, df_event, df_song, output_data):
     """
        This Function create dimentions and facts table from event and song data.
        Param:
            spark: spark session
            df_event: Dataframe from json event
            df_song: Dataframe from json song
            output_data: path to save the table on s3
    """
    # filter by actions for song plays
    df = df_event.filter(df_event.page == 'NextSong')

    # extract columns for users table   
    user_fields = ["user_id", "first_name", "last_name", "gender", "level"]
    users_table = df.select(user_fields).dropDuplicates(['user_id'])
    # write users table to parquet files
    user_out_path = str(output_data + 'users/users.parquet')
    users_table.write.parquet(user_out_path, mode = 'overwrite') 

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000), Timestamp())
    df = df.withColumn('timestamp', get_timestamp(df.ts))

    # extract columns to create time table
    time_table = df.select(
        col('timestamp').alias('start_time'),
        hour(col('timestamp')).alias('hour'),
        dayofmonth(col('timestamp')).alias('day'),
        weekofyear(col('timestamp')).alias('week'),
        month(col('timestamp')).alias('month'),
        year(col('timestamp')).alias('year')
    ).dropDuplicates(['start_time'])
    
    # write time table to parquet files partitioned by year and month
    time_out_path = str(output_data + 'time/time.parquet')
    time_table.write.parquet(time_out_path, mode = 'overwrite', partitionBy = ['year', 'month']) 
    
    # read in song data to use for songplays table

    df = df.join(df_song, (df_song.title==df.song)&(df_song.artist_name==df.artist)) \
        .withColumn('songplay_id', monotonically_increasing_id())

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = df.select(
        col('timestamp').alias('start_time'),
        col('userId'),
        col('level'),
        col('song_id'),
        col('artist_id'),
        col('sessionId'),
        col('location'),
        col('userAgent'),
        year('timestamp').alias('year'),
        month('timestamp').alias('month')
    )
    # write songplays table to parquet files partitioned by year and month
    songplay_out_path = str(output_data + 'songplays/songplays.parquet')
    songplays_table.write.parquet(songplay_out_path, mode='overwrite', partitionBy=['year', 'month'])


def main():
    input_access = "dl.cfg"
    set_aws_access(input_access)
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    #song_data = "song_data/A/B/C/TRABCEI128F424C983.json"
    song_data = "song_data/*/*/*/*.json"
    # get filepath to log data file
    #event_data = "log_data/2018/11/2018-11-13-events.json"
    event_data = "log_data/*/*/*.json"
    output_data = "s3a://data-engineer-data-lake/analytics/"
    
    song_schema = set_data_schema("Song")
    event_schema = set_data_schema("Event")

    df_song = readJsonFiles(spark, input_data + song_data, song_schema)
    df_event = readJsonFiles(spark, input_data + event_data, event_schema)
    #print(song_schema)
    process_song_data(spark, df_song, output_data)    
    process_log_data(spark, df_event, df_song, output_data)


if __name__ == "__main__":
    main() 
