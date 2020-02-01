
# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop     = "DROP TABLE IF EXISTS users;"
song_table_drop     = "DROP TABLE IF EXISTS songs; "
artist_table_drop   = "DROP TABLE IF EXISTS artists;"
time_table_drop     = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

songplay_table_create = ("""
   CREATE TABLE IF NOT EXISTS songplays(
                                           songplay_id  SERIAL,
                                           start_time  TEXT NOT NULL,
                                           user_id     TEXT NOT NULL,
                                           level       TEXT NOT NULL,
                                           song_id     TEXT NOT NULL,
                                           artist_id   TEXT NOT NULL,
                                           session_id  TEXT NOT NULL,
                                           location    TEXT NOT NULL,
                                           user_agent  TEXT NOT NULL,
                                           PRIMARY KEY (songplay_id)
 );""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
                                     user_id text NOT NULL,
                                     first_name TEXT NOT NULL,
                                     last_name TEXT NOT NULL,
                                     gender TEXT NOT NULL,
                                     level TEXT NOT NULL,
                                     PRIMARY KEY (user_id)
 );""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
                                      song_id text NOT NULL,
                                      title TEXT NOT NULL,
                                      artist_id text NOT NULL,
                                      year INT NOT NULL,
                                      duration float NOT NULL,
                                      PRIMARY KEY (song_id)
 );""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
                                        artist_id text NOT NULL,
                                        name TEXT NOT NULL,
                                        location TEXT,
                                        latitude text,
                                        longitude text,
                                        PRIMARY KEY (artist_id)
 );""")


time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
                                         start_time timestamptz NOT NULL,
                                         hour int NOT NULL,
                                         day int NOT NULL,
                                         week text NOT NULL,
                                         month int NOT NULL,
                                         year int NOT NULL,
                                         weekday int NOT NULL,
                                         PRIMARY KEY (start_time)
 );""")


# INSERT RECORDS

songplay_table_insert = ("""
  INSERT INTO songplays( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
  INSERT INTO users( user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (user_id)
    DO UPDATE SET   first_name = EXCLUDED.first_name
                    last_name = EXCLUDED.last_name
                    gender = EXCLUDED.gender
                    level = EXCLUDED.level
    ;

""")

song_table_insert = ("""
  INSERT INTO songs( song_id , title , artist_id , year , duration ) VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (song_id)
    DO UPDATE SET   title = EXCLUDED.title
                    artist_id = EXCLUDED.artist_id
                    year = EXCLUDED.year
                    duration = EXCLUDED.duration
  ;
""")


artist_table_insert = ("""
  INSERT INTO artists( artist_id, name, location, latitude, longitude ) VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (artist_id)
    DO UPDATE SET   name = EXCLUDED.name
                    location = EXCLUDED.location
                    latitude = EXCLUDED.latitude
                    longitude = EXCLUDED.longitude
 ;
""")


time_table_insert = ("""
  INSERT INTO time( start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (start_time)
    DO UPDATE SET   hour = EXCLUDED.hour
                    day = EXCLUDED.day
                    week = EXCLUDED.week
                    month = EXCLUDED.month
                    year = EXCLUDED.year
                    weekday = EXCLUDED.weekday
 ;
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, artists.artist_id
    FROM songs
        INNER JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title = %s AND artists.name = %s
""")

# QUERY LISTS
create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
