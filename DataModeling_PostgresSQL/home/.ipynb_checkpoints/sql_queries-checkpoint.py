
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
                                           start_time  text, 
                                           user_id     text, 
                                           level       TEXT, 
                                           song_id     text , 
                                           artist_id   text, 
                                           session_id  text, 
                                           location    TEXT, 
                                           user_agent  TEXT
                                          );""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users(
                                     user_id text, 
                                     first_name TEXT, 
                                     last_name TEXT, 
                                     gender TEXT, 
                                     level TEXT,
                                     PRIMARY KEY (user_id)
                                    );""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs(
                                      song_id text, 
                                      title TEXT, 
                                      artist_id text, 
                                      year INT, 
                                      duration float,
                                      PRIMARY KEY (song_id)
                                    );""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists(
                                        artist_id text, 
                                        name TEXT, 
                                        location TEXT, 
                                        latitude text, 
                                        longitude text,
                                        PRIMARY KEY (artist_id)
                                       );""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
                                         start_time timestamptz , 
                                         hour int, 
                                         day int, 
                                         week text, 
                                         month int, 
                                         year int, 
                                         weekday int,
                                         PRIMARY KEY (start_time)
                                    );""")


# INSERT RECORDS

songplay_table_insert = ("""
  INSERT INTO songplays( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) VALUES ( %s, %s, %s, %s, %s, %s, %s, %s)
""")

user_table_insert = ("""
  INSERT INTO users( user_id, first_name, last_name, gender, level) VALUES (%s, %s, %s, %s, %s) 
  ON CONFLICT (user_id) DO NOTHING;
""")

song_table_insert = ("""
  INSERT INTO songs( song_id , title , artist_id , year , duration ) VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (song_id) DO NOTHING;
""")

artist_table_insert = ("""
  INSERT INTO artists( artist_id, name, location, latitude, longitude ) VALUES (%s, %s, %s, %s, %s)
  ON CONFLICT (artist_id) DO NOTHING;
""")


time_table_insert = ("""
  INSERT INTO time( start_time, hour, day, week, month, year, weekday) VALUES (%s, %s, %s, %s, %s, %s, %s)
  ON CONFLICT (start_time) DO NOTHING;
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
