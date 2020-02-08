# Import Python packages
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv

def process_log():
    """ Find and Process all log data to create only one file . """
    # checking your current working directory
    #print(os.getcwd())
    # Get your current folder and subfolder event data
    filepath = os.getcwd() + '/event_data'

    # Create a for loop to create a list of files and collect each filepath
    for root, dirs, files in os.walk(filepath):

    # join the file path and roots with the subdirectories using glob
        file_path_list = glob.glob(os.path.join(root,'*'))
        #print(file_path_list)

    # initiating an empty list of rows that will be generated from each file
    full_data_rows_list = []

    # for every filepath in the file path list
    for f in file_path_list:

    # reading csv file
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile:
            # creating a csv reader object
            csvreader = csv.reader(csvfile)
            next(csvreader)

     # extracting each data row one by one and append it
            for line in csvreader:
                #print(line)
                full_data_rows_list.append(line)

    # uncomment the code below if you would like to get total number of rows
    #print(len(full_data_rows_list))
    # uncomment the code below if you would like to check to see what the list of event data rows will look like
    #print(full_data_rows_list)

    # creating a smaller event data csv file called event_datafile_full csv that will be used to insert data into the \
    # Apache Cassandra tables
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True)

    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName','length',\
                    'level','location','sessionid','song','userId'])
        for row in full_data_rows_list:
            if (row[0] == ''):
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[12], row[13], row[16]))


def process_event(file, session):

    """ Insert values from event file into music_library, artist_library and song_library tables. """
    # We have provided part of the code to set up the CSV file. Please complete the Apache Cassandra code below#
    file = 'event_datafile_new.csv'

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            ## TO-DO: Assign the INSERT statements into the `query` variable
            artist_name = line[0]
            firstname = line[1]
            iteminsession = int(line[3])
            lastname  = line[4]
            song_lenght = float(line[5])
            sessionid = int(line[8])
            song_title = line[9]
            userid = int(line[10])

            ## INSERT  into the `music_library`
            query_music_library = " INSERT INTO music_library (artist_name, song_title, song_lenght, sessionid, iteminsession)"
            query_music_library = query_music_library + " VALUES (%s, %s, %s, %s, %s)"
            ## Uncomment the code below If you would like to check to see what you will insert in the table
            #print((line[0], line[9], float(line[5]), int(line[8]), int(line[3])))
            try:
                session.execute(query_music_library, (artist_name, song_title, song_lenght, sessionid, iteminsession))
            except Exception as e:
                print("Error INSERT INTO music_library")
                print(e)
                break;

            ## INSERT  into the `artist_library`
            query_artist_library = " INSERT INTO artist_library (artist_name, song_title, firstname, lastname, iteminsession, userid, sessionid)"
            query_artist_library = query_artist_library + " VALUES (%s, %s, %s, %s, %s, %s, %s)"

            try:
                session.execute(query_artist_library, (artist_name, song_title, firstname, lastname, iteminsession, userid, sessionid))
            except Exception as e:
                print("Error INSERT INTO artist_library")
                print(e)
                break;

            ## INSERT  into the `song_library`
            query_song_library = " INSERT INTO song_library (song_title, firstname, lastname, userid)"
            query_song_library = query_song_library + " VALUES (%s, %s, %s, %s)"

            try:
                session.execute(query_song_library, (song_title, firstname, lastname, userid))
            except Exception as e:
                print("Error INSERT INTO song_library")
                print(e)
                break;

def select_values(session):
    """ Select values from our tables """
    ## Query 1:  Give me the artist, song title and song's length in the music app history that was heard during \
    ## sessionid = 338, and itemInSession = 4
    query = "select * from music_library WHERE sessionid = 338 and itemInSession = 4 "
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    print('Data from artist_library:')
    for row in rows:
        print (row.artist_name, row.song_title, row.song_lenght)

    ## Query 2: Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name)\
    ## for userid = 10, sessionid = 182
    query = "select * from artist_library WHERE userid = 10 and sessionid = 182 "
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    print('Data from artist_library:')
    for row in rows:
        print(row)
        print (row.artist_name, row.song_title, row.firstname, row.firstname)

    ## Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
    query = "select * from song_library WHERE song_title = 'All Hands Against His Own' "
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)
    print('Data from song_library:')
    for row in rows:
        #print(row)
        print (row.firstname, row.firstname)



def create_tables(session):
    """ Create tables  music_library, artist_library and song_library tables. """
    ## TO-DO: Query 3: Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'
    query = "CREATE TABLE IF NOT EXISTS song_library "
    query = query + "(song_title TEXT, firstName TEXT, lastname TEXT, userid INT, PRIMARY KEY (song_title, userid))"
    try:
        session.execute(query)
        print('## TABLE song_library WAS CREATED!')
    except Exception as e:
        print(e)


    query = "CREATE TABLE IF NOT EXISTS artist_library "
    query = query + "(artist_name TEXT, song_title TEXT, firstName TEXT, lastname TEXT, iteminsession INT, userid INT, sessionid INT, PRIMARY KEY ((userid, sessionid), iteminsession)) WITH CLUSTERING ORDER BY (iteminsession DESC);"
    try:
        session.execute(query)
        print('## TABLE artist_library WAS CREATED!')
    except Exception as e:
        print(e)


    query = "CREATE TABLE IF NOT EXISTS music_library "
    query = query + "(artist_name TEXT, song_title TEXT, song_lenght FLOAT, sessionid INT, iteminsession INT, PRIMARY KEY ((sessionid, iteminsession), song_title))"
    try:
        session.execute(query)
        print('## TABLE music_library WAS CREATED!')
    except Exception as e:
        print(e)

def drop_tables(session):
    query = "drop table song_library"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    query = "drop table artis_library"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

    query = "drop table music_library"
    try:
        rows = session.execute(query)
    except Exception as e:
        print(e)

def close_connection(session, cluster):
    session.shutdown()
    cluster.shutdown()

def create_keyspace(session):
    # TO-DO: Create a Keyspace
    try:
        session.execute("""
        CREATE KEYSPACE IF NOT EXISTS udacity
        WITH REPLICATION =
        { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
    )
        print('## Udacity KEYSPACE was created!')
    except Exception as e:
            print(e)


def main():
    # Process all logs files to our analysis
    process_log()

    from cassandra.cluster import Cluster
    try:
        cluster = Cluster(['127.0.0.1'])

        # To establish connection and begin executing queries, need a session
        session = cluster.connect()

        # To create keyspace udacity
        create_keyspace(session)
    except Exception as e:
        print(e)

    # Set KEYSPACE to the keyspace specified above
    try:
        session.set_keyspace('udacity')
        print('#Udacity KEYSPACE has been defined!')
        # Create tables for our analysis
        create_tables(session)
        # Insert values into our tables that were created on the step above
        process_event('event_datafile_new.csv', session)
        # Select values from our tables
        select_values(session)
    except Exception as e:
        print('Error set keyspace udacity!')
        print(e)

    try:
        close_connection(session, cluster)
        print('#Connection was closed!')
    except Exception as e:
        print('Error close_connection!')
        print(e)

if __name__ == "__main__":
    main()
