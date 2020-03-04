import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

DB_NAME= config.get("CLUSTER", "DB_NAME")
DB_USER= config.get("CLUSTER", "DB_USER")
DB_PASSWORD= config.get("CLUSTER", "DB_PASSWORD")
DB_PORT = config.get("CLUSTER", "DB_PORT")

ARN = config.get("IAM_ROLE", "ARN")

LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")


# DROP TABLES

staging_events_table_drop = ""
staging_songs_table_drop = ""
songplay_table_drop = ""
user_table_drop = ""
song_table_drop = ""
artist_table_drop = ""
time_table_drop = ""

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_event(
                                        artists TEXT,
                                        auth TEXT,
                                        first_name TEXT,
                                        gender TEXT,
                                        item_in_session INTEGER,
                                        last_name TEXT,
                                        lenght TEXT,
                                        level TEXT,
                                        location TEXT,
                                        method TEXT,
                                        page TEXT,
                                        registration TEXT,
                                        session_id INTEGER,
                                        song TEXT,
                                        status INTEGER,
                                        ts TEXT,
                                        user_agente TEXT,
                                        user_id INTEGER
    ) DISTKEY(ts) SORTKEY(user_id);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_song(
                                        num_songs INTEGER,
                                        artist_id TEXT , 
                                        artist_latitude TEXT, 
                                        artist_longitude TEXT, 
                                        artist_location TEXT,          
                                        artist_name TEXT, 
                                        song_id TEXT, 
                                        title TEXT, 
                                        duration FLOAT,
                                        year INTEGER
    ) DISTKEY(artist_id) SORTKEY(artist_id);

""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays(
                                        songplay_id  IDENTITY(0,1),
                                        start_time  TEXT NOT NULL,
                                        user_id     TEXT NOT NULL,
                                        level       TEXT NOT NULL,
                                        song_id     TEXT NOT NULL,
                                        artist_id   TEXT NOT NULL,
                                        session_id  TEXT NOT NULL,
                                        location    TEXT NOT NULL,
                                        user_agent  TEXT NOT NULL
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
                                        user_id TEXT NOT NULL,
                                        first_name TEXT NOT NULL,
                                        last_name TEXT NOT NULL,
                                        gender TEXT NOT NULL,
                                        level TEXT NOT NULL
 
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
                                        song_id TEXT NOT NULL,
                                        title TEXT NOT NULL,
                                        artist_id TEXT NOT NULL,
                                        year INT NOT NULL,
                                        duration float NOT NULL 
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
                                        artist_id TEXT NOT NULL,
                                        name TEXT NOT NULL,
                                        location TEXT,
                                        latitude TEXT,
                                        longitude TEXT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
                                        start_time timestamptz NOT NULL,
                                        hour int NOT NULL,
                                        day int NOT NULL,
                                        week TEXT NOT NULL,
                                        month int NOT NULL,
                                        year int NOT NULL,
                                        weekday int NOT NULL
    );
""")

# STAGING TABLES

staging_events_copy = ("""
""").format()

SQL_COPY = """
copy staging_event from 's3://udacity-dend/song_data' 
credentials 'aws_iam_role={}'
gzip region 'us-west-2';
        """.format(DWH_ROLE_ARN)

staging_songs_copy = ("""s3://udacity-dend/log_data
""").format()

# FINAL TABLES

songplay_table_insert = ("""
""")

user_table_insert = ("""
""")

song_table_insert = ("""
""")

artist_table_insert = ("""
""")

time_table_insert = ("""
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
