import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
   """ Insert values from song files into artist and song tables. """
    if filepath:
        # for each file in filepath we open and insert values in correct table
        for fl in filepath:
            # open song fil
            fl_df = pd.read_json(filepath , lines=True)
            for data in fl_df.values:

                artist_id = data[0]
                artist_lat  = data[1]
                artist_location = data[2]
                artist_lng  = data[3]
                artist_name = data[4]
                duration  = data[5]
                song_id   = data[7]
                title     = data[8]
                year      = data[9]

                # insert artist record
                cur.execute(artist_table_insert, [artist_id, artist_name, artist_location, artist_lat, artist_lng])
                # insert song record
                cur.execute(song_table_insert, [song_id, title, artist_id, year, duration])


def process_log_file(cur, filepath):
    """ Extract values from log files.

        1) Insert values in dimentions table
        2) Insert values in fact table
    """
    if filepath:
        # Dict utilized just to convert colum_labels to a dictonary with time_data information
        idx_column_labels = {
            0:'timestamp',
            1:'hour',
            2:'day',
            3:'week_year',
            4:'month',
            5:'year',
            6:'weekday'
        }

    # open log file
    fl_df = pd.read_json(filepath , lines=True)
    # filter by NextSong action
    log_next_song_df = fl_df.loc[fl_df['page'] == 'NextSong']
    t = pd.to_datetime(log_next_song_df['ts'])
    # Setup of the variables with correct datatype
    # convert timestamp column to datetime
    timestamp = (t)
    hour = (t.dt.hour)
    day = (t.dt.day)
    week_of_year = (t.dt.weekofyear)
    month = (t.dt.month)
    year = (t.dt.year)
    week_day = (t.dt.weekday)

    # Setup time_data to a list of list
    time_data = [
         timestamp.tolist(), hour.tolist(),
         day.tolist(), week_of_year.tolist(),
         month.tolist(), year.tolist(), week_day.tolist()
    ]

    column_labels = dict()
    # Combine time_data and column labels
    for i in range(0,len(time_data)):
        try:
            _column = idx_column_labels[i]
        except:
            print('List with more columns than expected!')
            break;

        column_labels[_column] = time_data[i]

    # insert time data records
    time_df = pd.DataFrame(data = column_labels)
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
    # load user table
    user_df = fl_df[['userId', 'firstName', 'lastName', 'gender', 'level']].copy(deep=False)
    user_df = user_df.drop_duplicates(keep = False, inplace = False)
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    #insert songplay record
    for index, row in fl_df.loc[fl_df['page'] == 'NextSong'].iterrows():
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist))
        results = cur.fetchone()
        if results:
            songid, artistid = results
            #print()
            songplay_data = [row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
            cur.execute(songplay_table_insert, songplay_data )
        else:
            songid, artistid = None, None



def process_data(cur, conn, filepath, func):
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        #print(datafile)
        #break;
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
