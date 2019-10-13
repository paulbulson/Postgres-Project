import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *

def process_song_file(cur, filepath):
    """ 
    Access JSON file describing artists/songs and load contents into a Postgres database 
  
    Loads (see sql_queries.py for the INSERT statements) the artists and songs table (see sql_queries.py for the CREATE statements) from the JSON file pointed
    to be by filepath. Please see http://millionsongdataset.com/ for more on the dataset.
    
    Example record: 
    {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Cursor for a connection to a Postgres database
    filepath (string): Path to the JSON file to be processed
  
    Returns: 
    Nothing  
    """
    
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id', 'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)

def process_log_file(cur, filepath):
    """ 
    Access JSON file describing songplays and load contents into a Postgres database 
  
    Loads (see sql_queries.py for the INSERT statements) the songplays, users, and time table (see sql_queries.py for the CREATE statements) from the JSON file pointed
    to be by filepath. Please see http://millionsongdataset.com/ for more on the dataset.
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Cursor for a connection to a Postgres database
    filepath (string): Path to the JSON file to be processed
  
    Returns: 
    Nothing
    """

    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data = list([df['ts'], t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday])
    column_labels = list(['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday'])

    # convert column labels and time data into a dictionary for easy conversion to a dataframe
    conv_dict = {column_labels[0]:time_data[0],
        column_labels[1]:time_data[1],
        column_labels[2]:time_data[2],
        column_labels[3]:time_data[3],
        column_labels[4]:time_data[4],
        column_labels[5]:time_data[5],
        column_labels[6]:time_data[6]
        }    
    time_df = pd.DataFrame(conv_dict)

    # insert time records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']].drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
                
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.length, row.artist))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, artistid, songid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """ 
    Locates the files to load based on filepath and submits them for load to the connection provided by the func provided. 

    Walks through directory structure to find all the json file (first loop) and submits for loading (second loop). 
    The Postgres table creation and data insert SQL is found in sql_queries.py.
    Please see http://millionsongdataset.com/ for more on the dataset.
  
    Parameters: 
    cur (psycopg2.extensions.cursor): Cursor for a connection to a Postgres database
    connection (psycopg2.extensions.connection): Connection to a Postgres database
    filepath (string): Path to the JSON file to be processed
    func (function): Function to process the file referenced by filepath
  
    Returns: 
    Nothing
    """

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
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """ 
    Creates a connection to the Postgres data and call the routines for processing the data. 
  
    Parameters: 
    None
    
    Returns: 
    Nothing
    """

    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    print(type(conn))
    cur = conn.cursor()
    print(type(cur))

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()