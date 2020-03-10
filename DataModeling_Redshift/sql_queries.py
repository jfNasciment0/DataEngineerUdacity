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
SONG_JSONPATH = config.get("S3", "SONG_JSONPATH")

# CREATE SCHEMA

db_schema_dw_music = "CREATE SCHEMA IF NOT EXISTS dw_music;"
db_schema_st_music = "CREATE SCHEMA IF NOT EXISTS st_music;"

# DROP TABLES

staging_events_table_drop = " DROP TABLE IF EXISTS sparkify.st_music.staging_event;"
staging_songs_table_drop = " DROP TABLE IF EXISTS sparkify.st_music.staging_song;"
songplay_table_drop = " DROP TABLE IF EXISTS sparkify.dw_music.songplays;"
user_table_drop = " DROP TABLE IF EXISTS sparkify.dw_music.users;"
song_table_drop = " DROP TABLE IF EXISTS sparkify.dw_music.songs;"
artist_table_drop = " DROP TABLE IF EXISTS sparkify.dw_music.artists;"
time_table_drop = " DROP TABLE IF EXISTS sparkify.dw_music.time;"

# SELECT TABLES 
staging_events_table_select = " SELECT COUNT(*) FROM sparkify.st_music.staging_event;"
staging_songs_table_select = " SELECT COUNT(*) FROM sparkify.st_music.staging_song;"
songplay_table_select = " SELECT COUNT(*) FROM sparkify.dw_music.songplays;"
user_table_select = " SELECT COUNT(*) FROM sparkify.dw_music.users;"
song_table_select = " SELECT COUNT(*) FROM sparkify.dw_music.songs;"
artist_table_select = " SELECT COUNT(*) FROM sparkify.dw_music.artists;"
time_table_select = " SELECT COUNT(*) FROM sparkify.dw_music.time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS sparkify.st_music.staging_event(
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
    CREATE TABLE IF NOT EXISTS sparkify.st_music.staging_song(
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
    CREATE TABLE IF NOT EXISTS sparkify.dw_music.songplays(
                                        songplay_id  INTEGER IDENTITY(0,1) PRIMARY KEY,
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
    CREATE TABLE IF NOT EXISTS sparkify.dw_music.users(
                                        user_id TEXT PRIMARY KEY,
                                        first_name TEXT NOT NULL,
                                        last_name TEXT NOT NULL,
                                        gender TEXT NOT NULL,
                                        level TEXT NOT NULL
 
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.dw_music.songs(
                                        song_id TEXT PRIMARY KEY,
                                        title TEXT NOT NULL,
                                        artist_id TEXT NOT NULL,
                                        year INT NOT NULL,
                                        duration float NOT NULL 
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.dw_music.artists(
                                        artist_id TEXT PRIMARY KEY,
                                        name TEXT NOT NULL,
                                        location TEXT,
                                        latitude TEXT,
                                        longitude TEXT
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS sparkify.dw_music.time(
                                        start_time timestamptz PRIMARY KEY,
                                        hour int NOT NULL,
                                        day int NOT NULL,
                                        week TEXT NOT NULL,
                                        month int NOT NULL,
                                        year int NOT NULL,
                                        weekday int NOT NULL
    );
""")

# STAGING TABLES

staging_songs_copy  = (""" 
COPY sparkify.st_music.staging_song from  '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS json '{}' ;
""").format(SONG_DATA, ARN, SONG_JSONPATH)

staging_events_copy = ("""
COPY sparkify.st_music.staging_event from '{}'
CREDENTIALS 'aws_iam_role={}'
REGION 'us-west-2'
FORMAT AS json '{}' ;
""").format(LOG_DATA, ARN, LOG_JSONPATH)


# FINAL TABLES

user_table_insert = ("""
begin transaction;

    delete from sparkify.dw_music.users 
    using sparkify.st_music.staging_event 
    where   staging_event.first_name = users.first_name
            and staging_event.last_name = users.last_name
            and staging_event.gender = users.gender
            and staging_event.level = users.level
    ; 

    insert into sparkify.dw_music.users 
    select user_id, first_name, last_name, gender, level 
        from sparkify.st_music.staging_event
        Where user_id is not null
    ;

end transaction;

""")

song_table_insert = ("""
begin transaction;

    delete from sparkify.dw_music.songs 
    using sparkify.st_music.staging_song 
    where   staging_song.title = songs.title
            and staging_song.artist_id = songs.artist_id
            and staging_song.year = songs.year
            and staging_song.duration = songs.duration
    ;

    insert into sparkify.dw_music.songs 
    select song_id , title , artist_id , year , duration 
        from sparkify.st_music.staging_song
        Where song_id is not null
    ;

end transaction;

""")

artist_table_insert = ("""
begin transaction;

    delete from sparkify.dw_music.artists 
    using sparkify.st_music.staging_song 
    where   staging_song.artist_name = artists.name
            and  staging_song.artist_latitude = artists.latitude
            and  staging_song.artist_longitude = artists.longitude
            and  staging_song.artist_location = artists.location
    ;

    insert into sparkify.dw_music.artists 
    select artist_id, artist_name, artist_location, artist_latitude, artist_longitude 
        from sparkify.st_music.staging_song
        where artist_id is not null
    ;

end transaction;

""")

time_table_insert = ("""
begin transaction;

    delete from sparkify.dw_music.time 
    using sparkify.st_music.staging_event 
    where        to_timestamp(ts::text, 'YYYYMMDDHH24MISS') = time.start_time
    ;

    insert into sparkify.dw_music.time 
    select 
            to_timestamp(ts::text, 'YYYYMMDDHH24MISS') 
            , EXTRACT(HOUR FROM to_timestamp(ts::text, 'YYYYMMDDHH24MISS'))
            , EXTRACT(DAY FROM to_timestamp(ts::text, 'YYYYMMDDHH24MISS'))
            , EXTRACT(WEEK FROM to_timestamp(ts::text, 'YYYYMMDDHH24MISS'))
            , EXTRACT(MONTH FROM to_timestamp(ts::text, 'YYYYMMDDHH24MISS'))
            , EXTRACT(YEAR FROM to_timestamp(ts::text, 'YYYYMMDDHH24MISS'))
            , EXTRACT(DOW FROM to_timestamp(ts::text, 'YYYYMMDDHH24MISS'))
        from sparkify.st_music.staging_event
        Where ts is not null
    ;

end transaction;
                   
""")

songplay_table_insert = ("""
begin transaction;

    insert into sparkify.dw_music.songplays  (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    select ts, user_id, level, song_id, staging_song.artist_id, session_id, location, user_agente
        from sparkify.st_music.staging_event 
        inner join sparkify.st_music.staging_song On staging_event.song = staging_song.title and
                                                     staging_event.artists = staging_song.artist_name
        WHERE staging_event.page = 'NextSong'
    ;

end transaction;
""")
# QUERY LISTS
create_schema_queries = [db_schema_dw_music, db_schema_st_music]
create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]
select_table_queries = [staging_events_table_select, staging_songs_table_select, songplay_table_select, user_table_select, song_table_select, artist_table_select, time_table_select]
